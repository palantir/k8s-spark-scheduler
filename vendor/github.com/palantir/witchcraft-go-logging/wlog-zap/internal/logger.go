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

package zapimpl

import (
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/palantir/witchcraft-go-logging/wlog"
	"github.com/palantir/witchcraft-go-logging/wlog-zap/internal/marshalers"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type zapLogEntry struct {
	fields []zapcore.Field
	wlog.MapValueEntries
}

func (e *zapLogEntry) StringValue(key, value string) {
	e.fields = append(e.fields, zap.String(key, value))
}

func (e *zapLogEntry) OptionalStringValue(key, value string) {
	if value != "" {
		e.StringValue(key, value)
	}
}

func (e *zapLogEntry) StringListValue(k string, v []string) {
	if len(v) > 0 {
		e.fields = append(e.fields, zap.Strings(k, v))
	}
}

func (e *zapLogEntry) SafeLongValue(key string, value int64) {
	e.fields = append(e.fields, zap.Int64(key, value))
}

func (e *zapLogEntry) IntValue(key string, value int32) {
	e.fields = append(e.fields, zap.Int32(key, value))
}

func (e *zapLogEntry) ObjectValue(k string, v interface{}, marshalerType reflect.Type) {
	if field, ok := marshalers.FieldForType(marshalerType, k, v); ok {
		e.fields = append(e.fields, field)
	} else {
		e.fields = append(e.fields, zap.Reflect(k, v))
	}
}

func (e *zapLogEntry) Fields() []zapcore.Field {
	fields := e.fields
	for key, values := range e.StringMapValues() {
		key := key
		values := values
		fields = append(fields, zap.Object(key, zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
			keys := make([]string, 0, len(values))
			for k := range values {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				enc.AddString(k, values[k])
			}
			return nil
		})))
	}
	for key, values := range e.AnyMapValues() {
		key := key
		values := values
		fields = append(fields, zap.Object(key, zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
			keys := make([]string, 0, len(values))
			for k := range values {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				if err := encodeField(k, values[k], enc); err != nil {
					return fmt.Errorf("failed to encode field %s: %v", k, err)
				}
			}
			return nil
		})))
	}
	return fields
}

type zapLogger struct {
	logger *zap.Logger
	level  *zap.AtomicLevel
}

func (l *zapLogger) Log(params ...wlog.Param) {
	logOutput(l.logger.Info, "", params)
}

func (l *zapLogger) Debug(msg string, params ...wlog.Param) {
	logOutput(l.logger.Debug, msg, params)
}

func (l *zapLogger) Info(msg string, params ...wlog.Param) {
	logOutput(l.logger.Info, msg, params)
}

func (l *zapLogger) Warn(msg string, params ...wlog.Param) {
	logOutput(l.logger.Warn, msg, params)
}

func (l *zapLogger) Error(msg string, params ...wlog.Param) {
	logOutput(l.logger.Error, msg, params)
}

func (l *zapLogger) SetLevel(level wlog.LogLevel) {
	l.level.SetLevel(toZapLevel(level))
}

func logOutput(logFn func(string, ...zap.Field), msg string, params []wlog.Param) {
	entry := &zapLogEntry{}
	wlog.ApplyParams(entry, params)
	logFn(msg, entry.Fields()...)
}

func encodeField(key string, value interface{}, enc zapcore.ObjectEncoder) error {
	switch v := value.(type) {
	case string:
		enc.AddString(key, v)
	case int:
		enc.AddInt(key, v)
	case int8:
		enc.AddInt8(key, v)
	case int16:
		enc.AddInt16(key, v)
	case int32:
		enc.AddInt32(key, v)
	case int64:
		enc.AddInt64(key, v)
	case uint:
		enc.AddUint(key, v)
	case uint8:
		enc.AddUint8(key, v)
	case uint16:
		enc.AddUint16(key, v)
	case uint32:
		enc.AddUint32(key, v)
	case uint64:
		enc.AddUint64(key, v)
	case bool:
		enc.AddBool(key, v)
	case float32:
		enc.AddFloat32(key, v)
	case float64:
		enc.AddFloat64(key, v)
	case []byte:
		enc.AddBinary(key, v)
	case time.Duration:
		enc.AddDuration(key, v)
	case time.Time:
		enc.AddTime(key, v)
		// support string and int slices explicitly because they are common slice types
	case []string:
		return enc.AddArray(key, zapcore.ArrayMarshalerFunc(func(enc zapcore.ArrayEncoder) error {
			for _, k := range v {
				enc.AppendString(k)
			}
			return nil
		}))
	case []int:
		return enc.AddArray(key, zapcore.ArrayMarshalerFunc(func(enc zapcore.ArrayEncoder) error {
			for _, k := range v {
				enc.AppendInt(k)
			}
			return nil
		}))
	default:
		// add non-primitive types using reflection
		return enc.AddReflected(key, v)
	}
	return nil
}
