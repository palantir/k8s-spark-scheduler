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

	"github.com/palantir/witchcraft-go-logging/wlog/reqlog/req2log"
	"github.com/palantir/witchcraft-go-server/witchcraft/internal/negroni"
	"github.com/palantir/witchcraft-go-server/wrouter"
	"github.com/palantir/witchcraft-go-tracing/wtracing"
	"github.com/palantir/witchcraft-go-tracing/wtracing/propagation/b3"
)

func NewRouteRequestLog(reqLogger req2log.Logger, baseParamPerms req2log.RequestParamPerms) wrouter.RouteHandlerMiddleware {
	return func(rw http.ResponseWriter, req *http.Request, reqVals wrouter.RequestVals, next wrouter.RouteRequestHandler) {
		lrw := toLoggingResponseWriter(rw)
		start := time.Now()
		next(lrw, req, reqVals)
		duration := time.Since(start)

		pathParamPerms := reqVals.ParamPerms.PathParamPerms()
		if baseParamPerms != nil {
			pathParamPerms = wrouter.NewCombinedParamPerms(baseParamPerms.PathParamPerms(), reqVals.ParamPerms.PathParamPerms())
		}
		queryParamPerms := reqVals.ParamPerms.QueryParamPerms()
		if baseParamPerms != nil {
			queryParamPerms = wrouter.NewCombinedParamPerms(baseParamPerms.QueryParamPerms(), reqVals.ParamPerms.QueryParamPerms())
		}
		headerParamPerms := reqVals.ParamPerms.HeaderParamPerms()
		if baseParamPerms != nil {
			headerParamPerms = wrouter.NewCombinedParamPerms(baseParamPerms.HeaderParamPerms(), reqVals.ParamPerms.HeaderParamPerms())
		}

		reqLogger.Request(req2log.Request{
			Request: req,
			RouteInfo: req2log.RouteInfo{
				Template:   reqVals.Spec.PathTemplate,
				PathParams: reqVals.PathParamVals,
			},
			ResponseStatus:   lrw.Status(),
			ResponseSize:     int64(lrw.Size()),
			Duration:         duration,
			PathParamPerms:   pathParamPerms,
			QueryParamPerms:  queryParamPerms,
			HeaderParamPerms: headerParamPerms,
		})
	}
}

func toLoggingResponseWriter(rw http.ResponseWriter) loggingResponseWriter {
	if lrw, ok := rw.(loggingResponseWriter); ok {
		return lrw
	}
	// if provided responseWriter does not implement loggingResponseWriter, wrap in a negroniResponseWriter,
	// which implements the interface. There is no particular reason that the default implementation is
	// negroni's beyond the fact that negroni already provides an implementation of the required interface.
	return negroni.NewResponseWriter(rw)
}

// loggingResponseWriter defines the functions required by the logging handler to get information on the status and
// size of the response written by a writer.
type loggingResponseWriter interface {
	http.ResponseWriter
	// Status returns the status code of the response or 0 if the response has not been written.
	Status() int
	// Size returns the size of the response body.
	Size() int
}

func NewRouteLogTraceSpan() wrouter.RouteHandlerMiddleware {
	return func(rw http.ResponseWriter, req *http.Request, reqVals wrouter.RequestVals, next wrouter.RouteRequestHandler) {
		tracer := wtracing.TracerFromContext(req.Context())
		if tracer == nil {
			next(rw, req, reqVals)
			return
		}

		// create new span and set it on the context and header
		spanName := req.Method
		if reqVals.Spec.PathTemplate != "" {
			spanName += " " + reqVals.Spec.PathTemplate
		}
		reqSpanCtx := b3.SpanExtractor(req)()
		span := tracer.StartSpan(spanName, wtracing.WithParentSpanContext(reqSpanCtx))
		defer span.Finish()

		ctx := req.Context()
		ctx = wtracing.ContextWithSpan(ctx, span)

		req = req.WithContext(ctx)
		b3.SpanInjector(req)(span.Context())

		next(rw, req, reqVals)
	}
}
