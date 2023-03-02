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
	"reflect"
	"testing"
	"time"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/config"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestResourcesSorting(t *testing.T) {
	var one = *resource.NewQuantity(1, resource.BinarySI)
	var two = *resource.NewQuantity(2, resource.BinarySI)

	var node = &resources.Resources{
		CPU:    one,
		Memory: one,
	}
	var freeMemory = &resources.Resources{
		CPU:    one,
		Memory: two,
	}

	if resourcesLessThan(freeMemory, node) || !resourcesLessThan(node, freeMemory) {
		t.Error("Nodes should be sorted by how much memory is available ascending")
	}
	var freeCPU = &resources.Resources{
		CPU:    two,
		Memory: one,
	}
	if resourcesLessThan(freeCPU, node) || !resourcesLessThan(node, freeCPU) {
		t.Error("If available memory is equal, nodes should be sorted by how much CPU is available ascending")
	}
}

func TestScheduleContextSorting(t *testing.T) {
	one := *resource.NewQuantity(1, resource.BinarySI)
	two := *resource.NewQuantity(2, resource.BinarySI)

	lessResources := &resources.Resources{
		CPU:    one,
		Memory: one,
	}
	moreResources := &resources.Resources{
		CPU:    one,
		Memory: two,
	}
	base1 := scheduleContext{
		azPriority:    0,
		nodeResources: lessResources,
		nodeName:      "base1",
	}
	base2 := scheduleContext{
		azPriority:    0,
		nodeResources: lessResources,
		nodeName:      "base2",
	}
	lowerAzPriority := scheduleContext{
		azPriority:    1,
		nodeResources: lessResources,
		nodeName:      "lower",
	}
	moreNodeResources := scheduleContext{
		azPriority:    0,
		nodeResources: moreResources,
		nodeName:      "more",
	}

	if scheduleContextLessThan(lowerAzPriority, base1) || !scheduleContextLessThan(base1, lowerAzPriority) {
		t.Error("Nodes should be sorted by the priority of the AZ they belong to, ascending")
	}

	if scheduleContextLessThan(moreNodeResources, base1) || !scheduleContextLessThan(base1, moreNodeResources) {
		t.Error("If AZ priority is equal, nodes should be sorted by the available resources, ascending")
	}

	if scheduleContextLessThan(base2, base1) || !scheduleContextLessThan(base1, base2) {
		t.Error("If everything is equal, nodes should be sorted by node name, ascending")
	}
}

func TestAZAwareNodeSorting(t *testing.T) {
	one := *resource.NewQuantity(1, resource.BinarySI)
	two := *resource.NewQuantity(2, resource.BinarySI)

	zone1Node1SchedulingMetadata := &resources.NodeSchedulingMetadata{
		AvailableResources: &resources.Resources{
			CPU:    one,
			Memory: one,
		},
		ZoneLabel: "zone1",
	}
	zone1Node2FreeMemory := &resources.NodeSchedulingMetadata{
		AvailableResources: &resources.Resources{
			CPU:    one,
			Memory: two,
		},
		ZoneLabel: "zone1",
	}
	zone1Node3FreeCPU := &resources.NodeSchedulingMetadata{
		AvailableResources: &resources.Resources{
			CPU:    two,
			Memory: one,
		},
		ZoneLabel: "zone1",
	}
	zone2Node1SchedulingMetadata := &resources.NodeSchedulingMetadata{
		AvailableResources: &resources.Resources{
			CPU:    one,
			Memory: one,
		},
		ZoneLabel: "zone2",
	}
	nodesSchedulingMetadata := resources.NodeGroupSchedulingMetadata{
		"zone1Node1": zone1Node1SchedulingMetadata,
		"zone1Node2": zone1Node2FreeMemory,
		"zone1Node3": zone1Node3FreeCPU,
		"zone2Node1": zone2Node1SchedulingMetadata,
	}
	actual := getNodeNamesInPriorityOrder(nodesSchedulingMetadata)

	expectedResult := []string{"zone2Node1", "zone1Node1", "zone1Node3", "zone1Node2"}

	compareActualToExpected(actual, expectedResult, t)
}

func TestAZAwareNodeSortingWorksIfZoneLabelIsMissing(t *testing.T) {
	one := *resource.NewQuantity(1, resource.BinarySI)
	two := *resource.NewQuantity(2, resource.BinarySI)

	node1SchedulingMetadata := &resources.NodeSchedulingMetadata{
		AvailableResources: &resources.Resources{
			CPU:    two,
			Memory: one,
		},
		CreationTimestamp: time.Now(),
		Ready:             true,
	}
	node2SchedulingMetadata := &resources.NodeSchedulingMetadata{
		AvailableResources: &resources.Resources{
			CPU:    two,
			Memory: two,
		},
		CreationTimestamp: time.Now(),
		Ready:             true,
	}
	node3SchedulingMetadata := &resources.NodeSchedulingMetadata{
		AvailableResources: &resources.Resources{
			CPU:    one,
			Memory: one,
		},
		CreationTimestamp: time.Now(),
		Ready:             true,
	}

	nodesSchedulingMetadata := resources.NodeGroupSchedulingMetadata{
		"node1": node1SchedulingMetadata,
		"node2": node2SchedulingMetadata,
		"node3": node3SchedulingMetadata,
	}
	actual := getNodeNamesInPriorityOrder(nodesSchedulingMetadata)

	expectedResult := []string{"node3", "node1", "node2"}

	compareActualToExpected(actual, expectedResult, t)
}

func compareActualToExpected(actualNodes []string, expectedResult []string, t *testing.T) {
	if len(actualNodes) != len(expectedResult) {
		t.Error("Length of nodes slice shouldn't change on sorting")
	}
	for i, expectedNode := range expectedResult {
		if expectedNode != actualNodes[i] {
			t.Error("Each element in the sorted result should match the expected result. ", expectedResult, actualNodes)
		}
	}
}

func TestLabelPrioritySorting(t *testing.T) {
	tests := []struct {
		name                  string
		labelPriorityOrder    *config.LabelPriorityOrder
		schedulingMetadata    resources.NodeGroupSchedulingMetadata
		nodeNames             []string
		expectedNodeNameOrder []string
	}{{
		name: "sorts when extra label values",
		labelPriorityOrder: &config.LabelPriorityOrder{
			Name:                     "test-label",
			DescendingPriorityValues: []string{"best", "good"},
		},
		schedulingMetadata: resources.NodeGroupSchedulingMetadata{
			"node1": {AllLabels: map[string]string{"test-label": "worst"}},
			"node2": {AllLabels: map[string]string{"test-label": "good"}},
			"node3": {AllLabels: map[string]string{"test-label": "best"}},
		},
		nodeNames:             []string{"node1", "node3", "node2"},
		expectedNodeNameOrder: []string{"node3", "node2", "node1"},
	}, {
		name: "sorts when there are extra nodes with no labels set",
		labelPriorityOrder: &config.LabelPriorityOrder{
			Name:                     "test-label",
			DescendingPriorityValues: []string{"best", "good"},
		},
		schedulingMetadata: resources.NodeGroupSchedulingMetadata{
			"node1": {AllLabels: map[string]string{}},
			"node2": {AllLabels: map[string]string{"test-label": "good"}},
			"node3": {AllLabels: map[string]string{"test-label": "best"}},
		},
		nodeNames:             []string{"node2", "node3", "node1"},
		expectedNodeNameOrder: []string{"node3", "node2", "node1"},
	}, {
		name: "sorts when all nodes have values with priorities set",
		labelPriorityOrder: &config.LabelPriorityOrder{
			Name:                     "test-label",
			DescendingPriorityValues: []string{"best", "better", "good"},
		},
		schedulingMetadata: resources.NodeGroupSchedulingMetadata{
			"node1": {AllLabels: map[string]string{"test-label": "better"}},
			"node2": {AllLabels: map[string]string{"test-label": "good"}},
			"node3": {AllLabels: map[string]string{"test-label": "best"}},
		},
		nodeNames:             []string{"node1", "node2", "node3"},
		expectedNodeNameOrder: []string{"node3", "node1", "node2"},
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fn := createLabelLessThanFunction(test.labelPriorityOrder)
			sortNodesByMetadataLessThanFunction(test.nodeNames, test.schedulingMetadata, fn)
			if !reflect.DeepEqual(test.nodeNames, test.expectedNodeNameOrder) {
				t.Errorf("Node order mismatch. Actual: %v Expected: %v", test.nodeNames, test.expectedNodeNameOrder)
			}
		})
	}

}
