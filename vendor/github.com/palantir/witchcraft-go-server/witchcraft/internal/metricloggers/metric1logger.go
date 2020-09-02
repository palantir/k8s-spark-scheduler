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
	"github.com/palantir/witchcraft-go-logging/wlog/metriclog/metric1log"
)

var _ metric1log.Logger = (*metric1Logger)(nil)

type metric1Logger struct {
	logger   metric1log.Logger
	recorder metricRecorder
}

func NewMetric1Logger(logger metric1log.Logger, registry metrics.Registry) metric1log.Logger {
	return &metric1Logger{
		logger:   logger,
		recorder: newMetricRecorder(registry, metric1log.TypeValue),
	}
}

func (m *metric1Logger) Metric(name, typ string, params ...metric1log.Param) {
	m.logger.Metric(name, typ, params...)
	m.recorder.RecordSLSLog()
}
