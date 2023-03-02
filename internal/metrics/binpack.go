// Copyright (c) 2019 Palantir Technologies. All rights reserved.
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

package metrics

import (
	"context"
	"math"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/binpack"
	"github.com/palantir/pkg/metrics"
)

const (
	packingEfficiencyMetricName = "foundry.spark.scheduler.packing_efficiency"

	packingResourceTagKey = "foundry.spark.scheduler.packing_resource"
	cpuTagValue           = "CPU"
	memoryTagValue        = "Memory"
	gpuTagValue           = "GPU"
	maxTagValue           = "Max"

	packingEfficiencyFunctionNameTagKey = "foundry.spark.scheduler.packing_function"

	// non-existing node to explicitly report average packing efficiency
	avgEfficiencyNodeName = "average"
)

var (
	cpuTag    = metrics.MustNewTag(packingResourceTagKey, cpuTagValue)
	memoryTag = metrics.MustNewTag(packingResourceTagKey, memoryTagValue)
	gpuTag    = metrics.MustNewTag(packingResourceTagKey, gpuTagValue)
	// represents higher of CPU and Memory packing efficiencies. GPU is explicitly excluded for now
	maxTag = metrics.MustNewTag(packingResourceTagKey, maxTagValue)
)

// ReportPackingEfficiency report packing efficiency metrics for a single packing result.
func ReportPackingEfficiency(
	ctx context.Context,
	packingFunctionName string,
	packingResult binpack.PackingResult) {

	contextTags := metrics.TagsFromContext(ctx)
	registry := metrics.NewRootMetricsRegistry()

	packingFunctionTag := metrics.MustNewTag(packingEfficiencyFunctionNameTagKey, packingFunctionName)

	// report packing efficiencies per node
	for _, efficiency := range packingResult.PackingEfficiencies {
		hostTag := HostTag(ctx, efficiency.NodeName)
		emitMetrics(registry, contextTags, hostTag, packingFunctionTag, efficiency)
	}

	// report avg packing efficiency for all nodes at once
	efficiency := packingResult.AvgPackingEfficiency
	avgHostTag := HostTag(ctx, avgEfficiencyNodeName)
	emitMetrics(registry, contextTags, avgHostTag, packingFunctionTag, &efficiency)
}

func emitMetrics(
	registry metrics.RootRegistry,
	contextTags metrics.Tags,
	hostTag metrics.Tag,
	packingFunctionTag metrics.Tag,
	efficiency *binpack.PackingEfficiency) {

	registry.GaugeFloat64(packingEfficiencyMetricName, append(contextTags, hostTag, packingFunctionTag, cpuTag)...).Update(efficiency.CPU)
	registry.GaugeFloat64(packingEfficiencyMetricName, append(contextTags, hostTag, packingFunctionTag, memoryTag)...).Update(efficiency.Memory)
	registry.GaugeFloat64(packingEfficiencyMetricName, append(contextTags, hostTag, packingFunctionTag, gpuTag)...).Update(efficiency.GPU)
	registry.GaugeFloat64(packingEfficiencyMetricName, append(contextTags, hostTag, packingFunctionTag, maxTag)...).Update(math.Max(efficiency.CPU, efficiency.Memory))
}
