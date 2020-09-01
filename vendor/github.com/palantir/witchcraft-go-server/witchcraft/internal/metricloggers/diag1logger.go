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
	"github.com/palantir/witchcraft-go-logging/conjure/witchcraft/api/logging"
	"github.com/palantir/witchcraft-go-logging/wlog/diaglog/diag1log"
)

var _ diag1log.Logger = (*diag1Logger)(nil)

type diag1Logger struct {
	logger   diag1log.Logger
	recorder metricRecorder
}

func NewDiag1Logger(logger diag1log.Logger, registry metrics.Registry) diag1log.Logger {
	return &diag1Logger{
		logger:   logger,
		recorder: newMetricRecorder(registry, diag1log.TypeValue),
	}
}

func (m *diag1Logger) Diagnostic(diagnostic logging.Diagnostic, params ...diag1log.Param) {
	m.logger.Diagnostic(diagnostic, params...)
	m.recorder.RecordSLSLog()
}
