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

// AzAwareTightlyPack is a SparkBinPackFunction that tries to put the driver pod
// to as prior nodes as possible before trying to tightly pack executors
// while also trying to ensure that we can fit everything in a single AZ.
// If it cannot fit into a single AZ it falls back to the TightlyPack SparkBinPackFunction.
var AzAwareTightlyPack = SparkBinPackFunction(func(
	ctx context.Context,
	driverResources, executorResources *resources.Resources,
	executorCount int,
	driverNodePriorityOrder, executorNodePriorityOrder []string,
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) *PackingResult {
	packingResult := SingleAZTightlyPack(ctx, driverResources, executorResources, executorCount, driverNodePriorityOrder, executorNodePriorityOrder, nodesSchedulingMetadata)
	if packingResult.HasCapacity {
		return packingResult
	}
	return SparkBinPack(ctx, driverResources, executorResources, executorCount, driverNodePriorityOrder, executorNodePriorityOrder, nodesSchedulingMetadata, tightlyPackExecutors)
})
