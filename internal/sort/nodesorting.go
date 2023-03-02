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

package sort

import (
	"sort"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/config"
)

// NodeSorter sorts nodes based on configured node labels and allocated resources.
type NodeSorter struct {
	driverNodePriorityLessThanFunction   func(*resources.NodeSchedulingMetadata, *resources.NodeSchedulingMetadata) bool
	executorNodePriorityLessThanFunction func(*resources.NodeSchedulingMetadata, *resources.NodeSchedulingMetadata) bool
}

// NewNodeSorter creates a new NodeSorter instance.
func NewNodeSorter(
	driverPrioritizedNodeLabel *config.LabelPriorityOrder,
	executorPrioritizedNodeLabel *config.LabelPriorityOrder) *NodeSorter {
	return &NodeSorter{
		driverNodePriorityLessThanFunction:   createLabelLessThanFunction(driverPrioritizedNodeLabel),
		executorNodePriorityLessThanFunction: createLabelLessThanFunction(executorPrioritizedNodeLabel),
	}
}

// PotentialNodes returns driver and executor nodes in priority order based on scheduling metadata and a list of candidate nodes for the driver.
func (n *NodeSorter) PotentialNodes(availableNodesSchedulingMetadata resources.NodeGroupSchedulingMetadata, nodeNames []string) (driverNodes, executorNodes []string) {
	nodesInPriorityOrder := getNodeNamesInPriorityOrder(availableNodesSchedulingMetadata)
	driverNodeNames := make([]string, 0, len(nodesInPriorityOrder))
	executorNodeNames := make([]string, 0, len(nodesInPriorityOrder))

	nodeNamesSet := make(map[string]interface{})
	for _, item := range nodeNames {
		nodeNamesSet[item] = nil
	}

	for _, nodeName := range nodesInPriorityOrder {
		if _, ok := nodeNamesSet[nodeName]; ok {
			driverNodeNames = append(driverNodeNames, nodeName)
		}
		if !availableNodesSchedulingMetadata[nodeName].Unschedulable && availableNodesSchedulingMetadata[nodeName].Ready {
			executorNodeNames = append(executorNodeNames, nodeName)
		}
	}

	// further sort driver and executor nodes based on config if present
	sortNodesByMetadataLessThanFunction(driverNodeNames, availableNodesSchedulingMetadata, n.driverNodePriorityLessThanFunction)
	sortNodesByMetadataLessThanFunction(executorNodeNames, availableNodesSchedulingMetadata, n.executorNodePriorityLessThanFunction)
	return driverNodeNames, executorNodeNames
}

type scheduleContext struct {
	// Lower value of priority indicates that the AZ has less resources
	azPriority    int
	nodeResources *resources.Resources
	nodeName      string
}

// Sort by available resources ascending, with RAM usage more important.
func resourcesLessThan(left *resources.Resources, right *resources.Resources) bool {
	var memoryCompared = left.Memory.Cmp(right.Memory)
	if memoryCompared != 0 {
		return memoryCompared == -1
	}
	return left.CPU.Cmp(right.CPU) == -1
}

// Sort first by AZ priority, then by resources on the node, then by node name
func scheduleContextLessThan(left, right scheduleContext) bool {
	if left.azPriority != right.azPriority {
		return left.azPriority < right.azPriority
	}

	if !left.nodeResources.Eq(right.nodeResources) {
		return resourcesLessThan(left.nodeResources, right.nodeResources)
	}

	return left.nodeName < right.nodeName
}

func getNodeNamesInPriorityOrder(nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) []string {
	nodeNames := getNodeNames(nodesSchedulingMetadata)

	var nodeNamesByAZ = groupNodeNamesByAZ(nodesSchedulingMetadata)
	var allAzLabels = getAllAZLabels(nodeNamesByAZ)
	var availableResourcesByAZ = getAvailableResourcesByAZ(nodeNamesByAZ, nodesSchedulingMetadata)

	sort.Slice(allAzLabels, func(i, j int) bool {
		return resourcesLessThan(availableResourcesByAZ[allAzLabels[i]], availableResourcesByAZ[allAzLabels[j]])
	})

	var scheduleContexts = make(map[string]scheduleContext, len(nodeNames))
	for azPriority, azLabel := range allAzLabels {
		for _, nodeName := range nodeNamesByAZ[azLabel] {
			scheduleContexts[nodeName] = scheduleContext{
				azPriority,
				nodesSchedulingMetadata[nodeName].AvailableResources,
				nodeName,
			}
		}
	}

	sort.Slice(nodeNames, func(i, j int) bool {
		return scheduleContextLessThan(scheduleContexts[nodeNames[i]], scheduleContexts[nodeNames[j]])
	})

	return nodeNames
}

func getAvailableResourcesByAZ(nodesByAZ map[string][]string, nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) map[string]*resources.Resources {
	var availableResourcesByAZ = make(map[string]*resources.Resources, len(nodesByAZ))
	for azLabel, nodesInAz := range nodesByAZ {
		var azResources = resources.Zero()
		for _, nodeName := range nodesInAz {
			azResources.Add(nodesSchedulingMetadata[nodeName].AvailableResources)
		}
		availableResourcesByAZ[azLabel] = azResources
	}
	return availableResourcesByAZ
}

func groupNodeNamesByAZ(nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) map[string][]string {
	nodesByAZ := make(map[string][]string)
	for nodeName, nodeSchedulingMetadata := range nodesSchedulingMetadata {
		azLabel := nodeSchedulingMetadata.ZoneLabel
		nodesByAZ[azLabel] = append(nodesByAZ[azLabel], nodeName)
	}
	return nodesByAZ
}

func getAllAZLabels(nodesByAZ map[string][]string) []string {
	azLabels := make([]string, 0)
	for key := range nodesByAZ {
		azLabels = append(azLabels, key)
	}
	return azLabels
}

func getNodeNames(nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) []string {
	nodeNames := make([]string, 0, len(nodesSchedulingMetadata))
	for key := range nodesSchedulingMetadata {
		nodeNames = append(nodeNames, key)
	}
	return nodeNames
}

func createLabelLessThanFunction(labelPriorityOrder *config.LabelPriorityOrder) func(*resources.NodeSchedulingMetadata, *resources.NodeSchedulingMetadata) bool {
	if labelPriorityOrder == nil {
		return nil
	}
	valueRanks := make(map[string]int, len(labelPriorityOrder.DescendingPriorityValues))
	for i, value := range labelPriorityOrder.DescendingPriorityValues {
		valueRanks[value] = i
	}
	return func(metadata1 *resources.NodeSchedulingMetadata, metadata2 *resources.NodeSchedulingMetadata) bool {
		rank1, ok := extractRank(metadata1.AllLabels, labelPriorityOrder.Name, valueRanks)
		if !ok {
			return false
		}
		rank2, ok := extractRank(metadata2.AllLabels, labelPriorityOrder.Name, valueRanks)
		if !ok {
			return true
		}
		return rank1 < rank2
	}
}

func extractRank(labels map[string]string, labelName string, knownRanks map[string]int) (int, bool) {
	if value, ok := labels[labelName]; ok {
		if rank, ok := knownRanks[value]; ok {
			return rank, true
		}
	}
	return 0, false
}

func sortNodesByMetadataLessThanFunction(
	nodeNames []string,
	metadata resources.NodeGroupSchedulingMetadata,
	lessThan func(*resources.NodeSchedulingMetadata, *resources.NodeSchedulingMetadata) bool) {
	if lessThan != nil {
		sort.SliceStable(nodeNames, func(i, j int) bool {
			return lessThan(metadata[nodeNames[i]], metadata[nodeNames[j]])
		})
	}
}
