// Copyright (c) 2018 Palantir Technologies. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package middleware

import (
	"net/http"
	"time"

	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog"
	"github.com/palantir/witchcraft-go-logging/wlog/auditlog/audit2log"
	"github.com/palantir/witchcraft-go-logging/wlog/evtlog/evt2log"
	"github.com/palantir/witchcraft-go-logging/wlog/extractor"
	"github.com/palantir/witchcraft-go-logging/wlog/metriclog/metric1log"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/trclog/trc1log"
	"github.com/palantir/witchcraft-go-server/witchcraft/internal/negroni"
	"github.com/palantir/witchcraft-go-server/wrouter"
	"github.com/palantir/witchcraft-go-tracing/wtracing"
	"github.com/palantir/witchcraft-go-tracing/wtracing/propagation/b3"
	"github.com/palantir/witchcraft-go-tracing/wzipkin"
)

func NewRequestPanicRecovery() wrouter.RequestHandlerMiddleware {
	return func(rw http.ResponseWriter, req *http.Request, next http.Handler) {
		recovery := negroni.NewRecovery()
		recovery.PrintStack = false
		recovery.ServeHTTP(rw, req, func(rw http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(rw, req)
		})
	}
}

// NewRequestContextLoggers is request middleware that sets loggers that can be retrieved from a context on the request
// context.
func NewRequestContextLoggers(
	svcLogger svc1log.Logger,
	evtLogger evt2log.Logger,
	auditLogger audit2log.Logger,
	metricLogger metric1log.Logger,
) wrouter.RequestHandlerMiddleware {
	return func(rw http.ResponseWriter, req *http.Request, next http.Handler) {
		ctx := req.Context()
		if svcLogger != nil {
			ctx = svc1log.WithLogger(ctx, svcLogger)
		}
		if evtLogger != nil {
			ctx = evt2log.WithLogger(ctx, evtLogger)
		}
		if auditLogger != nil {
			ctx = audit2log.WithLogger(ctx, auditLogger)
		}
		if metricLogger != nil {
			ctx = metric1log.WithLogger(ctx, metricLogger)
		}
		next.ServeHTTP(rw, req.WithContext(ctx))
	}
}

func NewRequestExtractIDs(
	svcLogger svc1log.Logger,
	trcLogger trc1log.Logger,
	tracerOptions []wtracing.TracerOption,
	idsExtractor extractor.IDsFromRequest,
) wrouter.RequestHandlerMiddleware {
	return func(rw http.ResponseWriter, req *http.Request, next http.Handler) {
		// extract all IDs from request
		ids := idsExtractor.ExtractIDs(req)
		uid := ids[extractor.UIDKey]
		sid := ids[extractor.SIDKey]
		tokenID := ids[extractor.TokenIDKey]

		// set IDs on context for loggers
		ctx := req.Context()
		if uid != "" {
			ctx = wlog.ContextWithUID(ctx, uid)
		}
		if sid != "" {
			ctx = wlog.ContextWithSID(ctx, sid)
		}
		if tokenID != "" {
			ctx = wlog.ContextWithTokenID(ctx, tokenID)
		}

		// create tracer and set on context. Tracer logs to trace logger if it is non-nil or is a no-op if nil.
		traceReporter := wtracing.NewNoopReporter()
		if trcLogger != nil {
			// add trc1logger with params set
			// TODO(nmiyake): there is currently ongoing discussion about whether or not these fields are required for trace logs. If they are not, it would cleaner to put the logic that extracts the IDs into its own request middleware layer.
			ctx = trc1log.WithLogger(ctx, trc1log.WithParams(trcLogger, trc1log.UID(uid), trc1log.SID(sid), trc1log.TokenID(tokenID)))
			traceReporter = trcLogger
		}
		tracer, err := wzipkin.NewTracer(traceReporter, tracerOptions...)
		if err != nil && svcLogger != nil {
			svcLogger.Error("Failed to create tracer", svc1log.Stacktrace(err))
		}
		ctx = wtracing.ContextWithTracer(ctx, tracer)

		// retrieve existing trace info from request and create a span
		reqSpanContext := b3.SpanExtractor(req)()
		span := tracer.StartSpan("witchcraft-go-server request middleware", wtracing.WithParentSpanContext(reqSpanContext))
		defer span.Finish()

		ctx = wtracing.ContextWithSpan(ctx, span)
		b3.SpanInjector(req)(span.Context())

		// update request with new context
		req = req.WithContext(ctx)

		// delegate to the next handler
		next.ServeHTTP(rw, req)
	}
}

// staticRootSpanIDGenerator returns the stored TraceID as its TraceID and SpanID.
type staticRootSpanIDGenerator wtracing.TraceID

func (s staticRootSpanIDGenerator) TraceID() wtracing.TraceID {
	return wtracing.TraceID(s)
}

func (s staticRootSpanIDGenerator) SpanID(traceID wtracing.TraceID) wtracing.SpanID {
	return wtracing.SpanID(s)
}

func NewRequestMetricRequestMeter(mr metrics.RootRegistry) wrouter.RequestHandlerMiddleware {
	const (
		serverResponseMetricName      = "server.response"
		serverResponseErrorMetricName = "server.response.error"
		serverRequestSizeMetricName   = "server.request.size"
		serverResponseSizeMetricName  = "server.response.size"
	)

	return func(rw http.ResponseWriter, r *http.Request, next http.Handler) {
		// add capability to store tags on the context
		r = r.WithContext(metrics.AddTags(r.Context()))

		start := time.Now()

		lrw := toLoggingResponseWriter(rw)
		next.ServeHTTP(lrw, r)

		tags := metrics.TagsFromContext(r.Context())

		// record metrics for call
		mr.Timer(serverResponseMetricName, tags...).Update(time.Since(start) / time.Microsecond)
		mr.Histogram(serverRequestSizeMetricName, tags...).Update(r.ContentLength)
		mr.Histogram(serverResponseSizeMetricName, tags...).Update(int64(lrw.Size()))
		if lrw.Status()/100 == 5 {
			mr.Meter(serverResponseErrorMetricName, tags...).Mark(1)
		}
	}
}
