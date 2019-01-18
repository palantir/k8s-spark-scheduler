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

package trc1log

import (
	"reflect"

	"github.com/palantir/witchcraft-go-logging/wlog"
	"github.com/palantir/witchcraft-go-tracing/wtracing"
)

type defaultLogger struct {
	logger wlog.Logger
}

var spanType = reflect.TypeOf((*wtracing.SpanModel)(nil)).Elem()

func (l *defaultLogger) Log(span wtracing.SpanModel, params ...Param) {
	l.logger.Log(
		append([]wlog.Param{
			wlog.NewParam(func(entry wlog.LogEntry) {
				entry.StringValue(wlog.TypeKey, TypeValue)
				entry.ObjectValue(SpanKey, span, spanType)
			}),
		}, l.toParams(params)...)...,
	)
}

func (l *defaultLogger) toParams(inParams []Param) []wlog.Param {
	if len(inParams) == 0 {
		return nil
	}
	outParams := make([]wlog.Param, len(inParams))
	for idx := range inParams {
		outParams[idx] = wlog.NewParam(inParams[idx].apply)
	}
	return outParams
}

func (l *defaultLogger) Send(span wtracing.SpanModel) {
	l.Log(span)
}

func (l *defaultLogger) Close() error {
	return nil
}
