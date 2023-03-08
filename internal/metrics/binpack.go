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
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/pkg/metrics"
)

const (
	packingEfficiencyMetricName = "foundry.spark.scheduler.packingefficiency"

	packingResourceTagKey = "foundry.spark.scheduler.packing_resource"
	cpuTagValue           = "CPU"
	memoryTagValue        = "Memory"
	gpuTagValue           = "GPU"
	maxTagValue           = "Max"

	packingEfficiencyFunctionNameTagKey = "foundry.spark.scheduler.packingfunction"
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
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	packingResult *binpack.PackingResult) {

	packingFunctionTag := metrics.MustNewTag(packingEfficiencyFunctionNameTagKey, packingFunctionName)

	// report avg packing efficiency for all nodes at once
	efficiency := computeAvgPackingEfficiencyForResult(nodesSchedulingMetadata, packingResult)
	emitMetrics(ctx, packingFunctionTag, efficiency)
}

func computeAvgPackingEfficiencyForResult(
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	packingResult *binpack.PackingResult) binpack.AvgPackingEfficiency {

	packingEfficienciesDefault := make([]*binpack.PackingEfficiency, 0)
	for _, packingEfficiency := range packingResult.PackingEfficiencies {
		packingEfficienciesDefault = append(packingEfficienciesDefault, packingEfficiency)
	}
	return binpack.ComputeAvgPackingEfficiency(nodesSchedulingMetadata, packingEfficienciesDefault)
}

func emitMetrics(
	ctx context.Context,
	packingFunctionTag metrics.Tag,
	efficiency binpack.AvgPackingEfficiency) {

	metrics.FromContext(ctx).GaugeFloat64(packingEfficiencyMetricName, packingFunctionTag, cpuTag).Update(efficiency.CPU)
	metrics.FromContext(ctx).GaugeFloat64(packingEfficiencyMetricName, packingFunctionTag, memoryTag).Update(efficiency.Memory)
	metrics.FromContext(ctx).GaugeFloat64(packingEfficiencyMetricName, packingFunctionTag, gpuTag).Update(efficiency.GPU)
	metrics.FromContext(ctx).GaugeFloat64(packingEfficiencyMetricName, packingFunctionTag, maxTag).Update(math.Max(efficiency.CPU, efficiency.Memory))
}
