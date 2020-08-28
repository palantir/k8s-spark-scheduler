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
	"github.com/palantir/witchcraft-go-logging/wlog/evtlog/evt2log"
)

var _ evt2log.Logger = (*evt2Logger)(nil)

type evt2Logger struct {
	logger   evt2log.Logger
	recorder metricRecorder
}

func NewEvt2Logger(logger evt2log.Logger, registry metrics.Registry) evt2log.Logger {
	return &evt2Logger{
		logger:   logger,
		recorder: newMetricRecorder(registry, evt2log.TypeValue),
	}
}

func (m *evt2Logger) Event(name string, params ...evt2log.Param) {
	m.logger.Event(name, params...)
	m.recorder.RecordSLSLog()
}
