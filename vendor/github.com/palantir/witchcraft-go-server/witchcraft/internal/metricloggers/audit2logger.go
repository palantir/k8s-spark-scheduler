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
	"github.com/palantir/witchcraft-go-logging/wlog/auditlog/audit2log"
)

var _ audit2log.Logger = (*audit2Logger)(nil)

type audit2Logger struct {
	logger   audit2log.Logger
	recorder metricRecorder
}

func NewAudit2Logger(logger audit2log.Logger, registry metrics.Registry) audit2log.Logger {
	return &audit2Logger{
		logger:   logger,
		recorder: newMetricRecorder(registry, audit2log.TypeValue),
	}
}

func (m *audit2Logger) Audit(name string, result audit2log.AuditResultType, params ...audit2log.Param) {
	m.logger.Audit(name, result, params...)
	m.recorder.RecordSLSLog()
}
