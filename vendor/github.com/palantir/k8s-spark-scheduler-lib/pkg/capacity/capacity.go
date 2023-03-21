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

package capacity

import (
	"math"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"gopkg.in/inf.v0"
	"k8s.io/apimachinery/pkg/api/resource"
)

// NodeAndExecutorCapacity is a pair of node name and capacity (how many executors can it fit)
type NodeAndExecutorCapacity struct {
	NodeName string
	Capacity int
}

// getCapacityAgainstSingleDimension computes how many times we can fit the required quantity within (available-reserved)
// e.g. if required = 4, available = 14, reserved = 1, we can fit 3 executors (3 * required <= available - reserved)
//
// This function is only useful to compare one dimension at a time, e.g. CPU or Memory, use GetNodeCapacity to account
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

// GetNodeCapacity returns how many singleExecutor can fit within available - reserved
func GetNodeCapacity(available, reserved, singleExecutor *resources.Resources) int {
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

// GetNodeCapacities return value is ordered according to nodePriorityOrder
func GetNodeCapacities(
	nodePriorityOrder []string,
	nodeGroupSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	reservedResources resources.NodeGroupResources,
	singleExecutor *resources.Resources,
) []NodeAndExecutorCapacity {
	capacities := make([]NodeAndExecutorCapacity, 0, len(nodePriorityOrder))

	for _, nodeName := range nodePriorityOrder {
		if nodeSchedulingMetadata, ok := nodeGroupSchedulingMetadata[nodeName]; ok {
			reserved := resources.Zero()

			if alreadyReserved, ok := reservedResources[nodeName]; ok {
				reserved = alreadyReserved
			}

			capacities = append(capacities, NodeAndExecutorCapacity{
				nodeName,
				GetNodeCapacity(nodeSchedulingMetadata.AvailableResources, reserved, singleExecutor),
			})
		}
	}

	return capacities
}

// FilterOutNodesWithoutCapacity returns a slice of nodes with non-zero capacity
func FilterOutNodesWithoutCapacity(capacities []NodeAndExecutorCapacity) []NodeAndExecutorCapacity {
	filteredCapacities := make([]NodeAndExecutorCapacity, 0, len(capacities))
	for _, nodeWithCapacity := range capacities {
		if nodeWithCapacity.Capacity > 0 {
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
