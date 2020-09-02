// Copyright (c) 2020 Palantir Technologies. All rights reserved.
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

package metricloggers

import (
	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog/trclog/trc1log"
	"github.com/palantir/witchcraft-go-tracing/wtracing"
)

var _ trc1log.Logger = (*trc1Logger)(nil)

type trc1Logger struct {
	logger   trc1log.Logger
	recorder metricRecorder
}

func NewTrc1Logger(logger trc1log.Logger, registry metrics.Registry) trc1log.Logger {
	return &trc1Logger{
		logger:   logger,
		recorder: newMetricRecorder(registry, trc1log.TypeValue),
	}
}

func (m *trc1Logger) Log(span wtracing.SpanModel, params ...trc1log.Param) {
	m.logger.Log(span, params...)
	m.recorder.RecordSLSLog()
}

func (m *trc1Logger) Send(sm wtracing.SpanModel) {
	m.logger.Send(sm)
}

func (m *trc1Logger) Close() error {
	return m.logger.Close()
}
