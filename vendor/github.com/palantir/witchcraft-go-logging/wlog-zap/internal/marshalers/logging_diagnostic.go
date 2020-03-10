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
	"fmt"

	"github.com/palantir/witchcraft-go-logging/conjure/witchcraft/api/logging"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func marshalLoggingDiagnostic(key string, val interface{}) zapcore.Field {
	return zap.Object(key, zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
		diagnostic := val.(logging.Diagnostic)
		return diagnostic.Accept(&marshalVisitor{
			enc: enc,
		})
	}))
}

type marshalVisitor struct {
	enc zapcore.ObjectEncoder
}

func (m *marshalVisitor) VisitGeneric(v logging.GenericDiagnostic) error {
	m.enc.AddString("type", "generic")
	return m.enc.AddObject("generic", zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
		enc.AddString("diagnosticType", v.DiagnosticType)
		return enc.AddReflected("value", v.Value)
	}))
}

func (m *marshalVisitor) VisitThreadDump(v logging.ThreadDumpV1) error {
	m.enc.AddString("type", "threadDump")
	return m.enc.AddObject("threadDump", zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
		return enc.AddArray("threads", zapcore.ArrayMarshalerFunc(func(enc zapcore.ArrayEncoder) error {
			for _, currThread := range v.Threads {
				if err := enc.AppendObject(threadInfoV1Encoder(currThread)); err != nil {
					return err
				}
			}
			return nil
		}))
	}))
}

func threadInfoV1Encoder(threadInfo logging.ThreadInfoV1) zapcore.ObjectMarshaler {
	return zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
		if threadInfo.Id != nil {
			enc.AddInt64("id", int64(*threadInfo.Id))
		}
		encodeNonEmptyString(enc, "name", threadInfo.Name)
		if len(threadInfo.StackTrace) > 0 {
			if err := enc.AddArray("stackTrace", zapcore.ArrayMarshalerFunc(func(enc zapcore.ArrayEncoder) error {
				for _, stackFrame := range threadInfo.StackTrace {
					if err := enc.AppendObject(stackFrameV1Encoder(stackFrame)); err != nil {
						return err
					}
				}
				return nil
			})); err != nil {
				return err
			}
		}
		if len(threadInfo.Params) > 0 {
			if err := enc.AddReflected("params", threadInfo.Params); err != nil {
				return err
			}
		}
		return nil
	})
}

func stackFrameV1Encoder(stackFrame logging.StackFrameV1) zapcore.ObjectMarshaler {
	return zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
		encodeNonEmptyString(enc, "address", stackFrame.Address)
		encodeNonEmptyString(enc, "procedure", stackFrame.Procedure)
		encodeNonEmptyString(enc, "file", stackFrame.File)
		if stackFrame.Line != nil {
			enc.AddInt("line", *stackFrame.Line)
		}
		if len(stackFrame.Params) > 0 {
			if err := enc.AddReflected("params", stackFrame.Params); err != nil {
				return err
			}
		}
		return nil
	})
}

func encodeNonEmptyString(enc zapcore.ObjectEncoder, key string, val *string) {
	if val == nil || len(*val) == 0 {
		return
	}
	enc.AddString(key, *val)
}

func (m *marshalVisitor) VisitUnknown(typeName string) error {
	return fmt.Errorf("unknown diagnostic type: %s", typeName)
}
