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

// DistributeEvenly is a SparkBinPackFunction that tries to put the driver pod
// to as prior nodes as possible before trying to distribute executors evenly
var DistributeEvenly = SparkBinPackFunction(func(
	ctx context.Context,
	driverResources, executorResources *resources.Resources,
	executorCount int,
	driverNodePriorityOrder, executorNodePriorityOrder []string,
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) (string, []string, bool) {
	return SparkBinPack(ctx, driverResources, executorResources, executorCount, driverNodePriorityOrder, executorNodePriorityOrder, nodesSchedulingMetadata, distributeExecutorsEvenly)
})

func distributeExecutorsEvenly(
	ctx context.Context,
	executorResources *resources.Resources,
	executorCount int,
	nodePriorityOrder []string,
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	reservedResources resources.NodeGroupResources) ([]string, bool) {
	availableNodes := make(map[string]bool, len(nodePriorityOrder))
	for _, name := range nodePriorityOrder {
		availableNodes[name] = true
	}
	executorNodes := make([]string, 0, executorCount)
	if executorCount == 0 {
		return executorNodes, true
	}
	for len(availableNodes) > 0 {
		for _, n := range nodePriorityOrder {
			if _, ok := availableNodes[n]; !ok {
				continue
			}

			if reservedResources[n] == nil {
				reservedResources[n] = resources.Zero()
			}
			reservedResources[n].Add(executorResources)
			nodeSchedulingMetadata, ok := nodesSchedulingMetadata[n]
			if !ok || reservedResources[n].GreaterThan(nodeSchedulingMetadata.AvailableResources) {
				// can not allocate a resource to this node
				delete(availableNodes, n)
			} else {
				executorNodes = append(executorNodes, n)
				if len(executorNodes) == executorCount {
					return executorNodes, true
				}
			}
		}
	}
	return nil, false
}
