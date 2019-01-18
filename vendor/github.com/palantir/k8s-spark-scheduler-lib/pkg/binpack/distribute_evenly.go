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
	availableResources resources.NodeGroupResources) (string, []string, bool) {
	return SparkBinPack(ctx, driverResources, executorResources, executorCount, driverNodePriorityOrder, executorNodePriorityOrder, availableResources, distributeExecutorsEvenly)
})

func distributeExecutorsEvenly(
	ctx context.Context,
	executorResources *resources.Resources,
	executorCount int,
	nodePriorityOrder []string,
	availableResources, reserved resources.NodeGroupResources) ([]string, bool) {
	availableNodes := make(map[string]bool, len(nodePriorityOrder))
	for _, name := range nodePriorityOrder {
		availableNodes[name] = true
	}
	ret := make([]string, 0, executorCount)
	if executorCount == 0 {
		return ret, true
	}
	for len(availableNodes) > 0 {
		for _, n := range nodePriorityOrder {
			if reserved[n] == nil {
				reserved[n] = resources.Zero()
			}
			reserved[n].Add(executorResources)
			if reserved[n].GreaterThan(availableResources[n]) {
				// can not allocate a resource to this node
				delete(availableNodes, n)
			} else {
				ret = append(ret, n)
				if len(ret) == executorCount {
					return ret, true
				}
			}
		}
	}
	return ret, false
}
