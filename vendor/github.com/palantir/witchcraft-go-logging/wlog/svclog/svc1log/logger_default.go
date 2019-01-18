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

package svc1log

import (
	"github.com/palantir/witchcraft-go-logging/wlog"
)

type defaultLogger struct {
	loggerCreator func(level wlog.LogLevel) wlog.LeveledLogger

	logger wlog.LeveledLogger
	params []Param
}

func (l *defaultLogger) Debug(msg string, params ...Param) {
	l.logger.Debug(msg, l.toParams(params)...)
}

func (l *defaultLogger) Info(msg string, params ...Param) {
	l.logger.Info(msg, l.toParams(params)...)
}

func (l *defaultLogger) Warn(msg string, params ...Param) {
	l.logger.Warn(msg, l.toParams(params)...)
}

func (l *defaultLogger) Error(msg string, params ...Param) {
	l.logger.Error(msg, l.toParams(params)...)
}

func (l *defaultLogger) SetLevel(level wlog.LogLevel) {
	l.logger.SetLevel(level)
}

func (l *defaultLogger) toParams(inParams []Param) []wlog.Param {
	if len(inParams) == 0 {
		return defaultTypeParam
	}
	outParams := make([]wlog.Param, len(defaultTypeParam)+len(inParams))
	copy(outParams, defaultTypeParam)
	for idx := range inParams {
		outParams[len(defaultTypeParam)+idx] = wlog.NewParam(inParams[idx].apply)
	}
	return outParams
}

var defaultTypeParam = []wlog.Param{
	wlog.NewParam(func(entry wlog.LogEntry) {
		entry.StringValue(wlog.TypeKey, TypeValue)
	}),
}
