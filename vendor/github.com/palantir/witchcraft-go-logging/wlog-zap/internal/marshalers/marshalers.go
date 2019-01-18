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
	"reflect"

	"github.com/palantir/witchcraft-go-logging/conjure/witchcraft/spec/logging"
	"github.com/palantir/witchcraft-go-tracing/wtracing"
	"go.uber.org/zap/zapcore"
)

type encoderFunc func(key string, val interface{}) zapcore.Field

var encoders = map[reflect.Type]encoderFunc{
	reflect.TypeOf(wtracing.SpanModel{}): marshalWTracingSpanModel,
	reflect.TypeOf(logging.Diagnostic{}): marshalLoggingDiagnostic,
}

func FieldForType(typ reflect.Type, key string, val interface{}) (zapcore.Field, bool) {
	fn, ok := encoders[typ]
	if !ok {
		return zapcore.Field{}, false
	}
	return fn(key, val), true
}
