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
	"io"

	"github.com/palantir/pkg/metrics"
)

var _ io.Writer = (*metricWriter)(nil)

type metricWriter struct {
	writer   io.Writer
	recorder metricRecorder
}

// NewMetricWriter returns a MetricWriter that delegates to writer, and records SLS log line length
// according to the provided slsFilename, e.g. "service", or "trace".
// If registry is nil, then no metrics are recorded.
func NewMetricWriter(writer io.Writer, registry metrics.Registry, slsFilename string) io.Writer {
	var recorder metricRecorder
	if registry != nil {
		recorder = newMetricRecorder(registry, slsFilename)
	}
	return &metricWriter{
		writer:   writer,
		recorder: recorder,
	}
}

func (m *metricWriter) Write(p []byte) (int, error) {
	n, err := m.writer.Write(p)
	if m.recorder != nil {
		m.recorder.RecordSLSLogLength(n)
	}
	return n, err
}
