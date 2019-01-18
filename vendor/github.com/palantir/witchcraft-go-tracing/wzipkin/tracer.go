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

package wzipkin

import (
	"fmt"

	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/palantir/witchcraft-go-tracing/wtracing"
)

func NewTracer(rep wtracing.Reporter, opts ...wtracing.TracerOption) (wtracing.Tracer, error) {
	zipkinReporter := newZipkinReporterAdapter(rep)
	zipkinTracerOpts := toZipkinTracerOptions(wtracing.FromTracerOptions(opts...))

	zipkinTracer, err := zipkin.NewTracer(zipkinReporter, zipkinTracerOpts...)
	if err != nil {
		return nil, err
	}

	return &tracerImpl{
		tracer: zipkinTracer,
		rootSpanTracerCreator: func(traceID model.TraceID) *zipkin.Tracer {
			// add option that sets ID generator to be a fixed one that returns provided TraceID and SpanID based on it
			opts := append(zipkinTracerOpts, zipkin.WithIDGenerator(fixedTraceIDRootSpanGenerator(traceID)))

			// known that error cannot be nil: if it were, the first construction should have returned a non-nil error
			zipkinTracer, _ := zipkin.NewTracer(zipkinReporter, opts...)
			return zipkinTracer
		},
	}, nil
}

func toZipkinTracerOptions(impl *wtracing.TracerOptionImpl) []zipkin.TracerOption {
	var zipkinTracerOptions []zipkin.TracerOption
	zipkinTracerOptions = append(zipkinTracerOptions, zipkin.WithSharedSpans(false))
	zipkinTracerOptions = append(zipkinTracerOptions, zipkin.WithLocalEndpoint(toZipkinEndpoint(impl.LocalEndpoint)))
	if impl.Sampler != nil {
		zipkinTracerOptions = append(zipkinTracerOptions, zipkin.WithSampler(zipkin.Sampler(impl.Sampler)))
	}
	return zipkinTracerOptions
}

type tracerImpl struct {
	// tracer is that standard tracer used to create spans. Used in all cases except when a span with custom properties
	// needs to be created: specifically, creating a new root span with a partial parent information (where the TraceID
	// needs to match the parent's TraceID and the SpanID must match that as well) requires custom tracer configuration.
	tracer *zipkin.Tracer

	// rootSpanTracerCreator is a function that creates a new tracer that, given a TraceID, returns a new *zipkin.Tracer
	// that creates spans that use the provided TraceID as the TraceID and a SpanID equal to the lower 64 bits of the
	// TraceID as its SpanID. The returned tracer should be configured in the same manner as the stored tracer except
	// for this aspect.
	rootSpanTracerCreator func(traceID model.TraceID) *zipkin.Tracer
}

func (t *tracerImpl) StartSpan(name string, options ...wtracing.SpanOption) wtracing.Span {
	wtracingSpanOptions := wtracing.FromSpanOptions(options...)
	zipkinSpanOptions := toZipkinSpanOptions(wtracingSpanOptions)

	tracer := t.tracer
	if parentSpan := wtracingSpanOptions.ParentSpan; parentSpan != nil &&
		parentSpan.TraceID != "" &&
		parentSpan.ID == "" {
		// parent span exists and has TraceID but no SpanID: create a new tracer that creates spans with TraceID and
		// SpanID that matches the TraceID of the parent
		traceID, err := model.TraceIDFromHex(string(parentSpan.TraceID))
		if err != nil {
			panic(fmt.Errorf("malformed TraceID %s: this should not be possible at this point. Error: %v", parentSpan.TraceID, err))
		}
		tracer = t.rootSpanTracerCreator(traceID)
	}
	return fromZipkinSpan(tracer.StartSpan(name, zipkinSpanOptions...))
}

func toZipkinEndpoint(endpoint *wtracing.Endpoint) *model.Endpoint {
	if endpoint == nil {
		return nil
	}
	return &model.Endpoint{
		ServiceName: endpoint.ServiceName,
		IPv4:        endpoint.IPv4,
		IPv6:        endpoint.IPv6,
		Port:        endpoint.Port,
	}
}

func fromZipkinEndpoint(endpoint *model.Endpoint) *wtracing.Endpoint {
	if endpoint == nil {
		return nil
	}
	return &wtracing.Endpoint{
		ServiceName: endpoint.ServiceName,
		IPv4:        endpoint.IPv4,
		IPv6:        endpoint.IPv6,
		Port:        endpoint.Port,
	}
}

type fixedTraceIDRootSpanGenerator model.TraceID

func (gen fixedTraceIDRootSpanGenerator) TraceID() model.TraceID {
	return model.TraceID(gen)
}

func (gen fixedTraceIDRootSpanGenerator) SpanID(traceID model.TraceID) model.ID {
	return model.ID(gen.Low)
}
