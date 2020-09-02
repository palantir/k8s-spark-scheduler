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
)

const (
	slsLoggingMeterName       = "logging.sls"
	slsLogLengthHistogramName = "logging.sls.length"
)

// map from level to tag for the level. Maps is used because these values are the only ones that are used, and
// pre-allocating them avoids the cost of calling metrics.MustNewTag on every invocation. This is worth optimizing since
// this is a known hot code path.
var levelTags = map[wlog.LogLevel]metrics.Tag{
	wlog.DebugLevel: createLevelTag(wlog.DebugLevel),
	wlog.InfoLevel:  createLevelTag(wlog.InfoLevel),
	wlog.WarnLevel:  createLevelTag(wlog.WarnLevel),
	wlog.ErrorLevel: createLevelTag(wlog.ErrorLevel),
	wlog.FatalLevel: createLevelTag(wlog.FatalLevel),
}

func createLevelTag(level wlog.LogLevel) metrics.Tag {
	return metrics.MustNewTag("level", string(level))
}

type metricRecorder interface {
	// RecordSLSLog increments the count of the SLS logging metric for the given log type.
	RecordSLSLog()

	// RecordSLSLog increments the count of the SLS logging metric for the given log type and log level. The level
	// parameter should be one of the wlog.LogLevel constants defined in the wlog package -- using a value that is not
	// defined there may cause this function to panic.
	RecordLeveledSLSLog(level wlog.LogLevel)

	// RecordSLSLogLength records the length of an SLS log line for the given log type.
	RecordSLSLogLength(len int)
}

type defaultMetricRecorder struct {
	registry metrics.Registry
	typeTag  metrics.Tag
}

func newMetricRecorder(registry metrics.Registry, typ string) metricRecorder {
	return &defaultMetricRecorder{
		registry: registry,
		typeTag:  metrics.MustNewTag("type", typ),
	}
}

func (m *defaultMetricRecorder) RecordSLSLogLength(len int) {
	m.registry.Histogram(slsLogLengthHistogramName, m.typeTag).Update(int64(len))
}

func (m *defaultMetricRecorder) RecordSLSLog() {
	m.registry.Meter(slsLoggingMeterName, m.typeTag).Mark(1)
}

func (m *defaultMetricRecorder) RecordLeveledSLSLog(level wlog.LogLevel) {
	levelTag, ok := levelTags[level]
	if !ok {
		// this should not happen since all levels should be in levelTags map, but handle just in case
		levelTag = createLevelTag(level)
	}
	m.registry.Meter(slsLoggingMeterName, m.typeTag, levelTag).Mark(1)
}
