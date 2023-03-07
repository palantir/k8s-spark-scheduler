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

func getSingleAZSparkBinFunction(fn GenericBinPackFunction) SparkBinPackFunction {
	return SparkBinPackFunction(func(
		ctx context.Context,
		driverResources, executorResources *resources.Resources,
		executorCount int,
		driverNodePriorityOrder, executorNodePriorityOrder []string,
		nodeGroupSchedulingMetadata resources.NodeGroupSchedulingMetadata) *PackingResult {

		driverZonesInOrder, driverNodePriorityOrderByZone := groupNodesByZone(driverNodePriorityOrder, nodeGroupSchedulingMetadata)
		_, executorNodePriorityOrderByZone := groupNodesByZone(executorNodePriorityOrder, nodeGroupSchedulingMetadata)

		packingResults := make([]*PackingResult, 0)

		for _, zone := range driverZonesInOrder {
			driverNodePriorityOrderForZone := driverNodePriorityOrderByZone[zone]
			executorNodePriorityOrderForZone, ok := executorNodePriorityOrderByZone[zone]
			if !ok {
				continue
			}
			packingResult := SparkBinPack(ctx, driverResources, executorResources, executorCount, driverNodePriorityOrderForZone, executorNodePriorityOrderForZone, nodeGroupSchedulingMetadata, fn)
			// consider all AZs
			if packingResult.HasCapacity {
				packingResults = append(packingResults, packingResult)
			}
		}

		if len(packingResults) == 0 {
			return EmptyPackingResult()
		}

		return chooseBestResult(nodeGroupSchedulingMetadata, packingResults)
	})
}

func groupNodesByZone(nodeNames []string, nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) ([]string, map[string][]string) {
	zonesInOrder := make([]string, 0)
	nodeNamesByZone := make(map[string][]string)
	for _, nodeName := range nodeNames {
		nodesSchedulingMetadata, ok := nodesSchedulingMetadata[nodeName]
		if !ok {
			continue
		}
		zoneLabel := nodesSchedulingMetadata.ZoneLabel
		if _, ok := nodeNamesByZone[zoneLabel]; !ok {
			zonesInOrder = append(zonesInOrder, zoneLabel)
		}
		nodeNamesByZone[zoneLabel] = append(nodeNamesByZone[zoneLabel], nodeName)
	}
	return zonesInOrder, nodeNamesByZone
}

// Chooses the result with the highest avg packing efficiency for the nodes we're scheduling onto.
func chooseBestResult(
	nodeGroupSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	results []*PackingResult) *PackingResult {

	bestResult := EmptyPackingResult()
	bestAvgPackingEfficiency := WorstAvgPackingEfficiency()

	for _, result := range results {
		nodeNames := append([]string{result.DriverNode}, result.ExecutorNodes...)
		nodePackingEfficiencies := make([]*PackingEfficiency, 0)
		for _, nodeName := range nodeNames {
			nodePackingEfficiency := result.PackingEfficiencies[nodeName]
			nodePackingEfficiencies = append(nodePackingEfficiencies, nodePackingEfficiency)
		}
		avgPackingEfficiency := ComputeAvgPackingEfficiency(nodeGroupSchedulingMetadata, nodePackingEfficiencies)
		if bestAvgPackingEfficiency.LessThan(avgPackingEfficiency) {
			bestResult = result
			bestAvgPackingEfficiency = avgPackingEfficiency
		}
	}

	return bestResult
}
