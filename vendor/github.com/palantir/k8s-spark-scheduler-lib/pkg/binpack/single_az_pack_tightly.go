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

// SingleAZTightlyPack ensures that the driver and all initially allocated executors (`spark-executor-count` or
// `spark-dynamic-allocation-min-executor-count` when dynamic allocation is used) are scheduled in the same AZ
// If these initial pods can not fit into the same AZ then the binpacking fails.
// It behaves identically to az-aware-tightly-pack otherwise.
// NOTE: This does not currently guarantee that subsequently requested executors (such as those scheduled from
// additional dynamically allocated pods or pre-empted pods) will be scheduled in the same AZ as the initially
// allocated pods
var SingleAZTightlyPack = SparkBinPackFunction(func(
	ctx context.Context,
	driverResources, executorResources *resources.Resources,
	executorCount int,
	driverNodePriorityOrder, executorNodePriorityOrder []string,
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) (string, []string, bool) {

	driverNodePriorityOrderByZone := groupNodesByZone(driverNodePriorityOrder, nodesSchedulingMetadata)
	executorNodePriorityOrderByZone := groupNodesByZone(executorNodePriorityOrder, nodesSchedulingMetadata)

	for zone, driverNodePriorityOrderForZone := range driverNodePriorityOrderByZone {
		executorNodePriorityOrderForZone, ok := executorNodePriorityOrderByZone[zone]
		if !ok {
			continue
		}
		driverNode, executorNodes, hasCapacity := SparkBinPack(ctx, driverResources, executorResources, executorCount, driverNodePriorityOrderForZone, executorNodePriorityOrderForZone, nodesSchedulingMetadata, tightlyPackExecutors)
		if hasCapacity {
			return driverNode, executorNodes, hasCapacity
		}
	}
	return "", nil, false
})

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
