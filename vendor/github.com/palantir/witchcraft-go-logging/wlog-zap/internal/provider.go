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
	"io"
	"time"

	"github.com/palantir/witchcraft-go-logging/wlog"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func LoggerProvider() wlog.LoggerProvider {
	return &loggerProvider{}
}

type loggerProvider struct{}

func (lp *loggerProvider) NewLogger(w io.Writer) wlog.Logger {
	logger, atomicLevel := newZapLogger(w, wlog.InfoLevel, zapcore.EncoderConfig{
		TimeKey:        wlog.TimeKey,
		EncodeTime:     rfc3339NanoTimeEncoder,
		EncodeDuration: zapcore.NanosDurationEncoder,
	})
	return &zapLogger{
		logger: logger,
		level:  atomicLevel,
	}
}

func (lp *loggerProvider) NewLeveledLogger(w io.Writer, level wlog.LogLevel) wlog.LeveledLogger {
	logger, atomicLevel := newZapLogger(w, level, zapcore.EncoderConfig{
		TimeKey:        wlog.TimeKey,
		EncodeTime:     rfc3339NanoTimeEncoder,
		EncodeDuration: zapcore.NanosDurationEncoder,
		LevelKey:       svc1log.LevelKey,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		MessageKey:     svc1log.MessageKey,
	})
	return &zapLogger{
		logger: logger,
		level:  atomicLevel,
	}
}

func rfc3339NanoTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.In(time.UTC).Format(time.RFC3339Nano))
}

func newZapLogger(w io.Writer, logLevel wlog.LogLevel, encoderConfig zapcore.EncoderConfig) (*zap.Logger, *zap.AtomicLevel) {
	level := zap.NewAtomicLevel()
	level.SetLevel(toZapLevel(logLevel))
	enc := zapcore.NewJSONEncoder(encoderConfig)
	return zap.New(zapcore.NewCore(enc, zapcore.AddSync(w), level)), &level
}

func toZapLevel(lvl wlog.LogLevel) zapcore.Level {
	switch lvl {
	case wlog.DebugLevel:
		return zapcore.DebugLevel
	case wlog.LogLevel(""), wlog.InfoLevel:
		return zapcore.InfoLevel
	case wlog.WarnLevel:
		return zapcore.WarnLevel
	case wlog.ErrorLevel:
		return zapcore.ErrorLevel
	case wlog.FatalLevel:
		return zapcore.FatalLevel
	default:
		panic(fmt.Errorf("Invalid log level %q", lvl))
	}
}
