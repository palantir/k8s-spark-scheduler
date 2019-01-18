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
	"strconv"

	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/palantir/witchcraft-go-tracing/wtracing"
)

func fromZipkinSpan(span zipkin.Span) wtracing.Span {
	return &spanImpl{
		span: span,
	}
}

type spanImpl struct {
	span zipkin.Span
}

func (s *spanImpl) Context() wtracing.SpanContext {
	return fromZipkinSpanContext(s.span.Context())
}

func (s *spanImpl) Finish() {
	s.span.Finish()
}

func fromZipkinSpanContext(spanCtx model.SpanContext) wtracing.SpanContext {
	var parentID *wtracing.SpanID
	if zipkinParentID := spanCtx.ParentID; zipkinParentID != nil && *zipkinParentID != 0 {
		parentIDStr := zipkinParentID.String()
		parentID = (*wtracing.SpanID)(&parentIDStr)
	}
	return wtracing.SpanContext{
		TraceID:  wtracing.TraceID(spanCtx.TraceID.String()),
		ID:       wtracing.SpanID(spanCtx.ID.String()),
		ParentID: parentID,
		Debug:    spanCtx.Debug,
		Sampled:  spanCtx.Sampled,
		Err:      spanCtx.Err,
	}
}

func fromZipkinSpanModel(spanModel model.SpanModel) wtracing.SpanModel {
	return wtracing.SpanModel{
		SpanContext:    fromZipkinSpanContext(spanModel.SpanContext),
		Name:           spanModel.Name,
		Kind:           wtracing.Kind(spanModel.Kind),
		Timestamp:      spanModel.Timestamp,
		Duration:       spanModel.Duration,
		LocalEndpoint:  fromZipkinEndpoint(spanModel.LocalEndpoint),
		RemoteEndpoint: fromZipkinEndpoint(spanModel.RemoteEndpoint),
	}
}

func toZipkinSpanOptions(impl *wtracing.SpanOptionImpl) []zipkin.SpanOption {
	var zipkinSpanOptions []zipkin.SpanOption
	zipkinSpanOptions = append(zipkinSpanOptions, zipkin.Kind(model.Kind(impl.Kind)))
	if re := impl.RemoteEndpoint; re != nil {
		zipkinSpanOptions = append(zipkinSpanOptions, zipkin.RemoteEndpoint(&model.Endpoint{
			ServiceName: re.ServiceName,
			IPv4:        re.IPv4,
			IPv6:        re.IPv6,
			Port:        re.Port,
		}))
	}
	if parent := impl.ParentSpan; parent != nil {
		zipkinSpanOptions = append(zipkinSpanOptions, zipkin.Parent(toZipkinSpanContext(*parent)))
	}
	return zipkinSpanOptions
}

func toZipkinSpanContext(sc wtracing.SpanContext) model.SpanContext {
	var err error

	var traceID model.TraceID
	if traceIDStrVal := string(sc.TraceID); traceIDStrVal != "" {
		traceID, err = model.TraceIDFromHex(traceIDStrVal)
		if err != nil {
			panic(fmt.Sprintf("TraceID() value %v returned by wtracing.SpanContext invalid: %v", traceIDStrVal, err))
		}
	}

	var spanID model.ID
	if spanIDStrVal := string(sc.ID); spanIDStrVal != "" {
		spanIDUintVal, err := strconv.ParseUint(spanIDStrVal, 16, 64)
		if err != nil {
			panic(fmt.Sprintf("ID() value %v returned by wtracing.SpanContext invalid: %v", spanIDStrVal, err))
		}
		spanID = model.ID(spanIDUintVal)
	}

	var parentID *model.ID
	if scParentID := sc.ParentID; scParentID != nil {
		parentIDStrVal := string(*scParentID)
		parentIDUIntVal, err := strconv.ParseUint(parentIDStrVal, 16, 64)
		if err != nil {
			panic(fmt.Sprintf("ParentID() value %v returned by wtracing.SpanContext invalid: %v", parentIDStrVal, err))
		}
		parentID = (*model.ID)(&parentIDUIntVal)
	}

	return model.SpanContext{
		TraceID:  traceID,
		ID:       spanID,
		ParentID: parentID,
		Debug:    sc.Debug,
		Sampled:  sc.Sampled,
		Err:      sc.Err,
	}
}
