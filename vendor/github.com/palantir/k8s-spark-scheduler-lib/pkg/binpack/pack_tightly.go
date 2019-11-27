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

package binpack

import (
	"context"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
)

// TightlyPack is a SparkBinPackFunction that tries to put the driver pod
// to as prior nodes as possible before trying to tightly pack executors
var TightlyPack = SparkBinPackFunction(func(
	ctx context.Context,
	driverResources, executorResources *resources.Resources,
	executorCount int,
	driverNodePriorityOrder, executorNodePriorityOrder []string,
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) (string, []string, bool) {
	return SparkBinPack(ctx, driverResources, executorResources, executorCount, driverNodePriorityOrder, executorNodePriorityOrder, nodesSchedulingMetadata, tightlyPackExecutors)
})

func tightlyPackExecutors(
	ctx context.Context,
	executorResources *resources.Resources,
	executorCount int,
	nodePriorityOrder []string,
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	reservedResources resources.NodeGroupResources) ([]string, bool) {
	executorNodes := make([]string, 0, executorCount)
	if executorCount == 0 {
		return executorNodes, true
	}
	for _, n := range nodePriorityOrder {
		if reservedResources[n] == nil {
			reservedResources[n] = resources.Zero()
		}
		for {
			reservedResources[n].Add(executorResources)
			nodeSchedulingMetadata, ok := nodesSchedulingMetadata[n]
			if !ok || reservedResources[n].GreaterThan(nodeSchedulingMetadata.AvailableResources) {
				break
			}
			executorNodes = append(executorNodes, n)
			if len(executorNodes) == executorCount {
				return executorNodes, true
			}
		}
	}
	return nil, false
}
