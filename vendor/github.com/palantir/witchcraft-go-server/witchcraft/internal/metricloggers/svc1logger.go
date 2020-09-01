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
	"github.com/palantir/witchcraft-go-logging/wlog"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
)

var _ svc1log.Logger = (*svc1Logger)(nil)

type svc1Logger struct {
	logger   svc1log.Logger
	recorder metricRecorder
}

func NewSvc1Logger(logger svc1log.Logger, registry metrics.Registry) svc1log.Logger {
	return &svc1Logger{
		logger:   logger,
		recorder: newMetricRecorder(registry, svc1log.TypeValue),
	}
}

func (m *svc1Logger) Debug(msg string, params ...svc1log.Param) {
	m.logger.Debug(msg, params...)
	m.recorder.RecordLeveledSLSLog(wlog.DebugLevel)
}

func (m *svc1Logger) Info(msg string, params ...svc1log.Param) {
	m.logger.Info(msg, params...)
	m.recorder.RecordLeveledSLSLog(wlog.InfoLevel)
}

func (m *svc1Logger) Warn(msg string, params ...svc1log.Param) {
	m.logger.Warn(msg, params...)
	m.recorder.RecordLeveledSLSLog(wlog.WarnLevel)
}

func (m *svc1Logger) Error(msg string, params ...svc1log.Param) {
	m.logger.Error(msg, params...)
	m.recorder.RecordLeveledSLSLog(wlog.ErrorLevel)
}

func (m *svc1Logger) SetLevel(level wlog.LogLevel) {
	m.logger.SetLevel(level)
}
