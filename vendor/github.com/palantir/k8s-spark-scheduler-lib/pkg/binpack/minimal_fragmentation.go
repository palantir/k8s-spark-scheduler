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
	"sort"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/capacity"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
)

// MinimalFragmentation is a SparkBinPackFunction that tries to minimize spark app fragmentation across the cluster.
// see minimalFragmentation for more details.
var MinimalFragmentation = SparkBinPackFunction(func(
	ctx context.Context,
	driverResources, executorResources *resources.Resources,
	executorCount int,
	driverNodePriorityOrder, executorNodePriorityOrder []string,
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) *PackingResult {
	return SparkBinPack(ctx, driverResources, executorResources, executorCount, driverNodePriorityOrder, executorNodePriorityOrder, nodesSchedulingMetadata, minimalFragmentation)
})

// minimalFragmentation attempts to pack executors onto as few nodes as possible, ideally a single one.
// nodePriorityOrder is still used as a guideline, i.e. if an application can fit on multiple nodes, it will pick
// the first eligible node according to nodePriorityOrder. additionally, minimalFragmentation will attempt to avoid
// mostly empty nodes unless those are required for scheduling or they provide a perfect fit, see a couple examples below.
//
// 'mostly' empty nodes are currently defined as the ones having capacity >= (executor count + max capacity) / 2
//
// for instance if nodePriorityOrder = [a, b, c, d, e, f]
// and we can fit 1 executor on a, 1 executor on b, 3 executors on c, 5 executors on d, 5 executors on e, 17 executors on f
// and executorCount = 11, then we will return:
// [d, d, d, d, d, e, e, e, e, e, a], true
//
// if instead we have executorCount = 6, then we will return:
// [d, d, d, d, d, a], true
//
// if instead we have executorCount = 15, then we will return:
// [d, d, d, d, d, e, e, e, e, e, c, c, c, a, b], true
//
// if instead we have executorCount = 17, then we will return:
// [f, f, ..., f], true
//
// if instead we have executorCount = 19, then we will return:
// [f, f, ..., f, a, b], true
func minimalFragmentation(
	_ context.Context,
	executorResources *resources.Resources,
	executorCount int,
	nodePriorityOrder []string,
	nodeGroupSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	reservedResources resources.NodeGroupResources) ([]string, bool) {
	if executorCount == 0 {
		return []string{}, true
	}

	nodeCapacities := capacity.GetNodeCapacities(nodePriorityOrder, nodeGroupSchedulingMetadata, reservedResources, executorResources)
	nodeCapacities = capacity.FilterOutNodesWithoutCapacity(nodeCapacities)
	if len(nodeCapacities) == 0 {
		return nil, false
	}

	sort.SliceStable(nodeCapacities, func(i, j int) bool {
		return nodeCapacities[i].Capacity < nodeCapacities[j].Capacity
	})
	maxCapacity := nodeCapacities[len(nodeCapacities)-1].Capacity
	if executorCount < maxCapacity {
		targetCapacity := (executorCount + maxCapacity) / 2
		firstNodeWithAtLeastTargetCapacity := sort.Search(len(nodeCapacities), func(i int) bool {
			return nodeCapacities[i].Capacity >= targetCapacity
		})

		// try scheduling on a subset of nodes that excludes the 'emptiest' nodes
		if executorNodes, ok := internalMinimalFragmentation(executorCount, nodeCapacities[:firstNodeWithAtLeastTargetCapacity]); ok {
			return executorNodes, ok
		}
	}

	// fall back to using empty nodes
	return internalMinimalFragmentation(executorCount, nodeCapacities)
}

func internalMinimalFragmentation(
	executorCount int,
	nodeCapacities []capacity.NodeAndExecutorCapacity) ([]string, bool) {
	nodeCapacitiesCopy := make([]capacity.NodeAndExecutorCapacity, 0, len(nodeCapacities))
	nodeCapacitiesCopy = append(nodeCapacitiesCopy, nodeCapacities...)
	executorNodes := make([]string, 0, executorCount)

	// as long as we have nodes where we could schedule executors
	for len(nodeCapacitiesCopy) > 0 {
		// pick the first node that could fit all the executors (if there's one)
		position := sort.Search(len(nodeCapacitiesCopy), func(i int) bool {
			return nodeCapacitiesCopy[i].Capacity >= executorCount
		})

		if position != len(nodeCapacitiesCopy) {
			// we found a node that has the required capacity, schedule everything there and we're done
			return append(executorNodes, repeat(nodeCapacitiesCopy[position].NodeName, executorCount)...), true
		}

		// we will need multiple nodes for scheduling, thus we'll try to schedule executors on nodes with the most capacity
		maxCapacity := nodeCapacitiesCopy[len(nodeCapacitiesCopy)-1].Capacity
		firstNodeWithMaxCapacityIdx := sort.Search(len(nodeCapacitiesCopy), func(i int) bool {
			return nodeCapacitiesCopy[i].Capacity >= maxCapacity
		})

		// the loop will exit because maxCapacity is always > 0
		currentPos := firstNodeWithMaxCapacityIdx
		for ; executorCount >= maxCapacity && currentPos < len(nodeCapacitiesCopy); currentPos++ {
			// we can skip the check on firstNodeWithMaxCapacityIdx since we know at least one node will be found
			executorNodes = append(executorNodes, repeat(nodeCapacitiesCopy[currentPos].NodeName, maxCapacity)...)
			executorCount -= maxCapacity
		}

		if executorCount == 0 {
			return executorNodes, true
		}

		nodeCapacitiesCopy = append(nodeCapacitiesCopy[:firstNodeWithMaxCapacityIdx], nodeCapacitiesCopy[currentPos:]...)
	}

	return nil, false
}

func repeat(str string, n int) []string {
	arr := make([]string, 0, n)
	for i := 0; i < n; i++ {
		arr = append(arr, str)
	}
	return arr
}
