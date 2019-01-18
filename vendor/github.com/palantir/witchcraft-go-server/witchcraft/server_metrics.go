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

package witchcraft

import (
	"context"

	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/metriclog/metric1log"
	"github.com/palantir/witchcraft-go-server/config"
)

func (s *Server) initMetrics(ctx context.Context, installCfg config.Install) (rRegistry metrics.RootRegistry, rDeferFn func(), rErr error) {
	metricsRegistry := metrics.DefaultMetricsRegistry
	metricsEmitFreq := defaultMetricEmitFrequency
	if freq := installCfg.MetricsEmitFrequency; freq > 0 {
		metricsEmitFreq = freq
	}

	// start routine that capture Go runtime metrics
	if !s.disableGoRuntimeMetrics {
		if ok := metrics.CaptureRuntimeMemStatsWithContext(ctx, metricsRegistry, metricsEmitFreq); !ok {
			return nil, nil, werror.Error("metricsRegistry does not support capturing runtime memory statistics")
		}
	}

	emitFn := func(metricID string, tags metrics.Tags, metricVal metrics.MetricVal) {
		s.metricLogger.Metric(metricID, metricVal.Type(), metric1log.Values(metricVal.Values()), metric1log.Tags(tags.ToMap()))
	}

	// start goroutine that logs metrics at the given frequency
	go metrics.RunEmittingRegistry(ctx, metricsRegistry, metricsEmitFreq, emitFn)

	return metricsRegistry, func() {
		// emit all metrics a final time on termination
		metricsRegistry.Each(emitFn)
	}, nil
}
