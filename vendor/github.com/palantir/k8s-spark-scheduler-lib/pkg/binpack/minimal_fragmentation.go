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
	"math"
	"sort"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"gopkg.in/inf.v0"
	"k8s.io/apimachinery/pkg/api/resource"
)

// MinimalFragmentation is a SparkBinPackFunction that tries to put the driver pod on the first possible node with
// enough capacity, and then tries to pack executors onto as few nodes as possible.
var MinimalFragmentation = SparkBinPackFunction(func(
	ctx context.Context,
	driverResources, executorResources *resources.Resources,
	executorCount int,
	driverNodePriorityOrder, executorNodePriorityOrder []string,
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) *PackingResult {
	return SparkBinPack(ctx, driverResources, executorResources, executorCount, driverNodePriorityOrder, executorNodePriorityOrder, nodesSchedulingMetadata, minimalFragmentation)
})

// minimalFragmentation attempts to pack executors onto as few nodes as possible, ideally a single one
// nodePriorityOrder is still used as a guideline, i.e. if an application can fit on multiple nodes, it will pick
// the first eligible node according to nodePriorityOrder
//
// for instance if nodePriorityOrder = [a, b, c, d, e]
// and we can fit 1 executor on a, 1 executor on b, 3 executors on c, 5 executors on d, 5 executors on e
// and executorCount = 11, then we will return:
// [d, d, d, d, d, e, e, e, e, e, a], true
//
// if instead we have executorCount = 6, then we will return:
// [d, d, d, d, d, a], true
func minimalFragmentation(
	_ context.Context,
	executorResources *resources.Resources,
	executorCount int,
	nodePriorityOrder []string,
	nodeGroupSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	reservedResources resources.NodeGroupResources) ([]string, bool) {
	executorNodes := make([]string, 0, executorCount)
	if executorCount == 0 {
		return executorNodes, true
	}

	nodeCapacities := getNodeCapacities(nodePriorityOrder, nodeGroupSchedulingMetadata, reservedResources, executorResources)
	nodeCapacities = filterOutNodesWithoutCapacity(nodeCapacities)
	sort.SliceStable(nodeCapacities, func(i, j int) bool {
		return nodeCapacities[i].capacity < nodeCapacities[j].capacity
	})

	// as long as we have nodes where we could schedule executors
	for len(nodeCapacities) > 0 {
		// pick the first node that could fit all the executors (if there's one)
		position := sort.Search(len(nodeCapacities), func(i int) bool {
			return nodeCapacities[i].capacity >= executorCount
		})

		if position != len(nodeCapacities) {
			// we found a node that has the required capacity, schedule everything there and we're done
			return append(executorNodes, repeat(nodeCapacities[position].nodeName, executorCount)...), true
		}

		// we will need multiple nodes for scheduling, thus we'll try to schedule executors on nodes with the most capacity
		maxCapacity := nodeCapacities[len(nodeCapacities)-1].capacity
		firstNodeWithMaxCapacityIdx := sort.Search(len(nodeCapacities), func(i int) bool {
			return nodeCapacities[i].capacity >= maxCapacity
		})

		// the loop will exit because maxCapacity is always > 0
		currentPos := firstNodeWithMaxCapacityIdx
		for ; executorCount >= maxCapacity && currentPos < len(nodeCapacities); currentPos++ {
			// we can skip the check on firstNodeWithMaxCapacityIdx since we know at least one node will be found
			executorNodes = append(executorNodes, repeat(nodeCapacities[currentPos].nodeName, maxCapacity)...)
			executorCount -= maxCapacity
		}

		if executorCount == 0 {
			return executorNodes, true
		}

		nodeCapacities = append(nodeCapacities[:firstNodeWithMaxCapacityIdx], nodeCapacities[currentPos:]...)
	}

	return nil, false
}

type nodeAndExecutorCapacity struct {
	nodeName string
	capacity int
}

// getCapacityAgainstSingleDimension computes how many times we can fit the required quantity within (available-reserved)
// e.g. if required = 4, available = 14, reserved = 1, we can fit 3 executors (3 * required <= available - reserved)
//
// This function is only useful to compare one dimension at a time, e.g. CPU or Memory, use getNodeCapacity to account
// for all dimensions
func getCapacityAgainstSingleDimension(available, reserved, required resource.Quantity) int {
	if reserved.Cmp(available) == 1 {
		// ideally this shouldn't happen (reserved > available), but should this happen, let's be resilient
		return 0
	}

	if required.IsZero() {
		// if we don't require any resources for this dimension, then we can fit an infinite number of executors
		return math.MaxInt
	}

	// this basically computes: floor((available - reserved) / required)
	return int(new(inf.Dec).QuoRound(
		new(inf.Dec).Sub(available.AsDec(), reserved.AsDec()),
		required.AsDec(),
		0,
		inf.RoundFloor,
	).UnscaledBig().Int64())
}

func getNodeCapacity(available, reserved, singleExecutor *resources.Resources) int {
	capacityConsideringCPUOnly := getCapacityAgainstSingleDimension(
		available.CPU,
		reserved.CPU,
		singleExecutor.CPU,
	)
	capacityConsideringMemoryOnly := getCapacityAgainstSingleDimension(
		available.Memory,
		reserved.Memory,
		singleExecutor.Memory,
	)
	capacityConsideringNvidiaGPUOnly := getCapacityAgainstSingleDimension(
		available.NvidiaGPU,
		reserved.NvidiaGPU,
		singleExecutor.NvidiaGPU,
	)

	return min(capacityConsideringCPUOnly, capacityConsideringMemoryOnly, capacityConsideringNvidiaGPUOnly)
}

// getNodeCapacities' return value is ordered according to nodePriorityOrder
func getNodeCapacities(
	nodePriorityOrder []string,
	nodeGroupSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	reservedResources resources.NodeGroupResources,
	singleExecutor *resources.Resources,
) []nodeAndExecutorCapacity {
	capacities := make([]nodeAndExecutorCapacity, 0, len(nodePriorityOrder))

	for _, nodeName := range nodePriorityOrder {
		if nodeSchedulingMetadata, ok := nodeGroupSchedulingMetadata[nodeName]; ok {
			reserved := resources.Zero()

			if alreadyReserved, ok := reservedResources[nodeName]; ok {
				reserved = alreadyReserved
			}

			capacities = append(capacities, nodeAndExecutorCapacity{
				nodeName,
				getNodeCapacity(nodeSchedulingMetadata.AvailableResources, reserved, singleExecutor),
			})
		}
	}

	return capacities
}

func filterOutNodesWithoutCapacity(capacities []nodeAndExecutorCapacity) []nodeAndExecutorCapacity {
	filteredCapacities := make([]nodeAndExecutorCapacity, 0, len(capacities))
	for _, nodeWithCapacity := range capacities {
		if nodeWithCapacity.capacity > 0 {
			filteredCapacities = append(filteredCapacities, nodeWithCapacity)
		}
	}
	return filteredCapacities
}

func min(a, b, c int) int {
	if a <= b && a <= c {
		return a
	} else if b <= c {
		return b
	}
	return c
}

func repeat(str string, n int) []string {
	arr := make([]string, 0, n)
	for i := 0; i < n; i++ {
		arr = append(arr, str)
	}
	return arr
}
