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

// Copyright (c) 2023 Palantir Technologies. All rights reserved.
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

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
)

// PackFewestBins is a SparkBinPackFunction that tries to put the driver pod
// to as prior nodes as possible before trying to pack executors into the fewest number of bins possible
var PackFewestBins = SparkBinPackFunction(func(
	ctx context.Context,
	driverResources, executorResources *resources.Resources,
	executorCount int,
	driverNodePriorityOrder, executorNodePriorityOrder []string,
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) (string, []string, bool) {
	return SparkBinPack(ctx, driverResources, executorResources, executorCount, driverNodePriorityOrder, executorNodePriorityOrder, nodesSchedulingMetadata, packExecutorsFewestBins)
})

type orderableNode struct {
	name               string
	canFitAllExecutors bool
	availableResources *resources.Resources
}

// packExecutorsFewestBins tries to pack executors into the fewest number of bins possible. Note that it takes node
// priority into account, but would pick nodes that fit more executors over the ones with higher priority. If there is
// a single note that can fit all executors, node with the fewest available resources will take priority.
func packExecutorsFewestBins(
	ctx context.Context,
	executorResources *resources.Resources,
	executorCount int,
	nodePriorityOrder []string,
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	reservedResources resources.NodeGroupResources) ([]string, bool) {
	totalExecutorResources := resources.Zero()
	for i := 0; i < executorCount; i++ {
		totalExecutorResources.Add(executorResources)
	}

	nodesToOrder := make([]orderableNode, 0, executorCount)
	for _, nodeName := range nodePriorityOrder {
		nodeResources := nodesSchedulingMetadata[nodeName].AvailableResources
		nodesToOrder = append(nodesToOrder, orderableNode{
			name:               nodeName,
			canFitAllExecutors: nodeResources.GreaterThan(totalExecutorResources) || nodeResources.Eq(totalExecutorResources),
			availableResources: nodeResources,
		})
	}

	sort.SliceStable(nodesToOrder, func(i, j int) bool {
		return nodesToOrder[i].availableResources.GreaterThan(nodesToOrder[j].availableResources)
	})

	smallestThatFitsAllExecutors := -1
	// TODO binary search
	for j, node := range nodesToOrder {
		if node.canFitAllExecutors {
			smallestThatFitsAllExecutors = j
		} else {
			break
		}
	}

	if smallestThatFitsAllExecutors != -1 {
		nodesToOrder = nodesToOrder[smallestThatFitsAllExecutors:]
	}

	orderedNodes := make([]string, 0, len(nodesToOrder))
	for _, node := range nodesToOrder {
		orderedNodes = append(orderedNodes, node.name)
	}

	return tightlyPackExecutors(ctx, executorResources, executorCount, orderedNodes, nodesSchedulingMetadata, reservedResources)
}
