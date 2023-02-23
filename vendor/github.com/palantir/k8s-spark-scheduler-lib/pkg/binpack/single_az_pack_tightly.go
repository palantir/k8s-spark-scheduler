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

// SingleAZTightlyPack is a SparkBinPackFunction that tries to put the driver pod
// to as prior nodes as possible before trying to tightly pack executors
// while also ensuring that we can fit everything in a single AZ.
// If it cannot fit into a single AZ binpacking fails
var SingleAZTightlyPack = SparkBinPackFunction(func(
	ctx context.Context,
	driverResources, executorResources *resources.Resources,
	executorCount int,
	driverNodePriorityOrder, executorNodePriorityOrder []string,
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) (string, []string, bool) {

	driverNodePriorityOrderByZone := groupNodesByZone(driverNodePriorityOrder, nodesSchedulingMetadata)
	executorNodePriorityOrderByZone := groupNodesByZone(executorNodePriorityOrder, nodesSchedulingMetadata)

	postBinpackNodeSpaceByAz := make(map[string]*resources.Resources)
	for zone, driverNodePriorityOrderForZone := range driverNodePriorityOrderByZone {
		executorNodePriorityOrderForZone, ok := executorNodePriorityOrderByZone[zone]
		if !ok {
			continue
		}
		driverNode, executorNodes, hasCapacity := SparkBinPack(ctx, driverResources, executorResources, executorCount, driverNodePriorityOrderForZone, executorNodePriorityOrderForZone, nodesSchedulingMetadata, tightlyPackExecutors)
		if !hasCapacity {
			continue
		}

		postBinpackNodeSpaceByAz[zone] = nodeSpacePostBinPack(nodesSchedulingMetadata, driverNode, driverResources, executorNodes, executorResources, executorCount)


	}

	return driverNode, executorNodes, hasCapacity

	return "", nil, false
})

func nodeSpacePostBinPack(nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	driverNode string, driverResources *resources.Resources,
	executorNodes []string, executorResources *resources.Resources, executorCount int) *resources.Resources {
	remainingDriverResources := nodesSchedulingMetadata[driverNode].AvailableResources.Copy()
	remainingDriverResources.Sub(driverResources)
	remainingExecutorResources := resources.Zero()
	for _, execNode := range executorNodes {
		remainingExecutorResources.Add(nodesSchedulingMetadata[execNode].AvailableResources)
	}
	for i := 0; i < executorCount; i++ {
		remainingExecutorResources.Sub(executorResources)
	}
	remainingDriverResources.Add(remainingExecutorResources)
	return remainingExecutorResources
}

func groupNodesByZone(nodeNames []string, nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) map[string][]string {
	nodeNamesByZone := make(map[string][]string)
	for _, nodeName := range nodeNames {
		nodesSchedulingMetadata, ok := nodesSchedulingMetadata[nodeName]
		if !ok {
			continue
		}
		zoneLabel := nodesSchedulingMetadata.ZoneLabel
		nodeNamesByZone[zoneLabel] = append(nodeNamesByZone[zoneLabel], nodeName)
	}
	return nodeNamesByZone
}
