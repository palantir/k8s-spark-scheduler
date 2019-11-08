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

package extender

import (
	"sort"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	v1 "k8s.io/api/core/v1"
)

const (
	zonePlaceholder = "default"
)

type scheduleContext struct {
	// Lower value of priority indicates that the AZ has less resources
	azPriority    int
	nodeResources *resources.Resources
}

// Sort by available resources ascending, with RAM usage more important.
func resourcesLessThan(left *resources.Resources, right *resources.Resources) bool {
	var memoryCompared = left.Memory.Cmp(right.Memory)
	if memoryCompared != 0 {
		return memoryCompared == -1
	}
	return left.CPU.Cmp(right.CPU) == -1
}

// Sort first by AZ priority and then by resources on the node
func scheduleContextLessThan(left scheduleContext, right scheduleContext) bool {
	if left.azPriority != right.azPriority {
		return left.azPriority < right.azPriority
	}
	return resourcesLessThan(left.nodeResources, right.nodeResources)
}

func sortNodes(useExperimentalHostPriorities bool, nodes []*v1.Node, availableResources resources.NodeGroupResources) {
	if !useExperimentalHostPriorities {
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[j].CreationTimestamp.Before(&nodes[i].CreationTimestamp)
		})
		return
	}

	var nodesByAZ = groupNodesByAZ(nodes)
	var allAzLabels = getAllAZLabels(nodesByAZ)
	var availableResourcesByAZ = getAvailableResourcesByAZ(nodesByAZ, availableResources)

	sort.Slice(allAzLabels, func(i, j int) bool {
		return resourcesLessThan(availableResourcesByAZ[allAzLabels[i]], availableResourcesByAZ[allAzLabels[j]])
	})

	var scheduleContexts = make(map[string]scheduleContext, len(nodes))
	for azPriority, azLabel := range allAzLabels {
		for _, node := range nodesByAZ[azLabel] {
			scheduleContexts[node.Name] = scheduleContext{
				azPriority,
				availableResources[node.Name],
			}
		}
	}

	sort.Slice(nodes, func(i, j int) bool {
		return scheduleContextLessThan(scheduleContexts[nodes[i].Name], scheduleContexts[nodes[j].Name])
	})
}

func getAvailableResourcesByAZ(nodesByAZ map[string][]*v1.Node, availableResources resources.NodeGroupResources) map[string]*resources.Resources {
	var availableResourcesByAZ = make(map[string]*resources.Resources, len(nodesByAZ))
	for azLabel, nodesInAz := range nodesByAZ {
		var azResources = resources.Zero()
		for _, node := range nodesInAz {
			azResources.Add(availableResources[node.Name])
		}
		availableResourcesByAZ[azLabel] = azResources
	}
	return availableResourcesByAZ
}

func groupNodesByAZ(nodes []*v1.Node) map[string][]*v1.Node {
	nodesByAZ := make(map[string][]*v1.Node)
	for i, node := range nodes {
		azLabel, ok := node.Labels[v1.LabelZoneFailureDomain]
		if !ok {
			azLabel = zonePlaceholder
		}
		nodesByAZ[azLabel] = append(nodesByAZ[azLabel], nodes[i])
	}
	return nodesByAZ
}

func getAllAZLabels(nodeGroupsByAZ map[string][]*v1.Node) []string {
	azLabels := make([]string, 0, len(nodeGroupsByAZ))
	for key := range nodeGroupsByAZ {
		azLabels = append(azLabels, key)
	}
	return azLabels
}
