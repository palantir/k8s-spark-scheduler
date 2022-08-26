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
	"io"
	"time"

	"github.com/palantir/witchcraft-go-logging/wlog"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func LoggerProvider() wlog.LoggerProvider {
	return &loggerProvider{}
}

type loggerProvider struct{}

func (lp *loggerProvider) NewLogger(w io.Writer) wlog.Logger {
	return &zapLogger{
		logger: newZapLogger(w, zapcore.EncoderConfig{
			EncodeTime:     rfc3339NanoTimeEncoder,
			EncodeDuration: zapcore.NanosDurationEncoder,
		}),
	}
}

func (lp *loggerProvider) NewLeveledLogger(w io.Writer, level wlog.LogLevel) wlog.LeveledLogger {
	return &zapLogger{
		logger: newZapLogger(w, zapcore.EncoderConfig{
			EncodeTime:     rfc3339NanoTimeEncoder,
			EncodeDuration: zapcore.NanosDurationEncoder,
			EncodeLevel:    zapcore.CapitalLevelEncoder,
		}),
		AtomicLogLevel: wlog.NewAtomicLogLevel(level),
	}
}

func rfc3339NanoTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.In(time.UTC).Format(time.RFC3339Nano))
}

func newZapLogger(w io.Writer, encoderConfig zapcore.EncoderConfig) *zap.Logger {
	// *zapLogger performs its own enforcement in the level-specific methods; no need for zap to check again.
	level := zap.LevelEnablerFunc(func(zapcore.Level) bool { return true })

	return zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(encoderConfig), zapcore.AddSync(w), level))
}
