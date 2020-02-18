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

package marshalers

import (
	"time"

	"github.com/palantir/witchcraft-go-logging/wlog"
	"github.com/palantir/witchcraft-go-logging/wlog/trclog/trc1log"
	"github.com/palantir/witchcraft-go-tracing/wtracing"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func marshalWTracingSpanModel(key string, val interface{}) zapcore.Field {
	return zap.Object(key, zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
		span := val.(wtracing.SpanModel)
		enc.AddString(wlog.TraceIDKey, string(span.TraceID))
		enc.AddString(trc1log.SpanIDKey, string(span.ID))
		enc.AddString(trc1log.SpanNameKey, span.Name)

		if parentID := span.ParentID; parentID != nil {
			enc.AddString(trc1log.SpanParentIDKey, string(*parentID))
		}
		enc.AddInt64(trc1log.SpanTimestampKey, span.Timestamp.Round(time.Microsecond).UnixNano()/1e3)
		enc.AddInt64(trc1log.SpanDurationKey, int64(span.Duration/time.Microsecond))
		if kind := span.Kind; kind != "" {
			// if kind is non-empty, manually create v1-style annotations
			switch kind {
			case wtracing.Server:
				if err := encodeSpanModelAnnotations(enc, "sr", "ss", span); err != nil {
					return err
				}
			case wtracing.Client:
				if err := encodeSpanModelAnnotations(enc, "cs", "cr", span); err != nil {
					return err
				}
			}
		}
		return nil
	}))
}

func encodeSpanModelAnnotations(enc zapcore.ObjectEncoder, startVal, endVal string, span wtracing.SpanModel) error {
	return enc.AddArray(trc1log.SpanAnnotationsKey, zapcore.ArrayMarshalerFunc(func(arrayEnc zapcore.ArrayEncoder) error {
		// add "sr" annotation
		if err := arrayEnc.AppendObject(spanModelAnnotationEncoder(startVal, span.Timestamp, span.LocalEndpoint)); err != nil {
			return err
		}
		// add "ss" annotation
		return arrayEnc.AppendObject(spanModelAnnotationEncoder(endVal, span.Timestamp.Add(span.Duration), span.LocalEndpoint))
	}))
}

func spanModelAnnotationEncoder(value string, timeStamp time.Time, endpoint *wtracing.Endpoint) zapcore.ObjectMarshaler {
	return zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
		enc.AddString(trc1log.AnnotationValueKey, value)
		enc.AddInt64(trc1log.AnnotationTimestampKey, timeStamp.Round(time.Microsecond).UnixNano()/1e3)
		if endpoint == nil {
			endpoint = &wtracing.Endpoint{}
		}
		return enc.AddObject(trc1log.AnnotationEndpointKey, zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
			enc.AddString(trc1log.EndpointServiceNameKey, endpoint.ServiceName)
			if len(endpoint.IPv4) > 0 {
				enc.AddString(trc1log.EndpointIPv4Key, endpoint.IPv4.String())
			}
			if len(endpoint.IPv6) > 0 {
				enc.AddString(trc1log.EndpointIPv6Key, endpoint.IPv6.String())
			}
			return nil
		}))
	})
}
