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

const (
	packingEfficiencyMetricName = "foundry.spark.scheduler.packing_efficiency"

	packingResourceTagKey = "foundry.spark.scheduler.packing_resource"
	cpuTagValue           = "CPU"
	memoryTagValue        = "Memory"
	gpuTagValue           = "GPU"
)

/*

const (
	scaleUpRateLimitBurstMetricName = "deployability.scaler.scale_up_rate_limit_burst"
	packingEfficiencyMetricName     = "deployability.scaler.packing_efficiency"

	scaleOperationTagKey = "deployability.scaler.scale_operation"
	scaleUpTagValue      = "scaleUp"
	scaleDownTagValue    = "scaleDown"

	binUIDTagKey          = "deployability.scaler.bin_uid"
	binUIDTagDefaultValue = "default"

	zoneTagKey          = "deployability.scaler.zone"
	zoneTagDefaultValue = "default"

	packingResourceTagKey = "deployability.scaler.packing_resource"
	cpuTagValue           = "CPU"
	memoryTagValue        = "Memory"
	gpuTagValue           = "GPU"
)

func packingEfficiency(ctx context.Context, bins []binpack.Bin, operationTag metrics.Tag) {
	contextTags := metrics.TagsFromContext(ctx)
	registry := metrics.NewRootMetricsRegistry()
	for _, bin := range bins {
		binTag := metrics.NewTagWithFallbackValue(binUIDTagKey, bin.UID, binUIDTagDefaultValue)
		zoneTag := metrics.NewTagWithFallbackValue(zoneTagKey, string(bin.Zone), zoneTagDefaultValue)
		efficiency := binpack.CalculateEfficiency(bin)
		registry.GaugeFloat64(packingEfficiencyMetricName, append(contextTags, binTag, zoneTag, operationTag, cpuTag)...).Update(efficiency.CPU)
		registry.GaugeFloat64(packingEfficiencyMetricName, append(contextTags, binTag, zoneTag, operationTag, memoryTag)...).Update(efficiency.Memory)
		registry.GaugeFloat64(packingEfficiencyMetricName, append(contextTags, binTag, zoneTag, operationTag, gpuTag)...).Update(efficiency.GPU)
	}
	emitAllMetrics(ctx, registry)
}
*/
