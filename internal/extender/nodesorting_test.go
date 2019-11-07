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
	"testing"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	useExperimentalPriorities = true
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
	base := scheduleContext{
		azPriority:    0,
		nodeResources: lessResources,
	}
	lowerAzPriority := scheduleContext{
		azPriority:    1,
		nodeResources: lessResources,
	}
	moreNodeResources := scheduleContext{
		azPriority:    0,
		nodeResources: moreResources,
	}

	if scheduleContextLessThan(lowerAzPriority, base) || !scheduleContextLessThan(base, lowerAzPriority) {
		t.Error("Nodes should be sorted by the priority of the AZ they belong to, ascending")
	}

	if scheduleContextLessThan(moreNodeResources, base) || !scheduleContextLessThan(base, moreNodeResources) {
		t.Error("If AZ priority is equal, nodes should be sorted by the available resources, ascending")
	}
}

func TestAZAwareNodeSorting(t *testing.T) {
	zone1Node1 := getNodeWithZoneLabel("zone1Node1", "zone1")
	zone1Node2 := getNodeWithZoneLabel("zone1Node2", "zone1")
	zone1Node3 := getNodeWithZoneLabel("zone1Node3", "zone1")
	zone2Node1 := getNodeWithZoneLabel("zone2Node1", "zone2")

	one := *resource.NewQuantity(1, resource.BinarySI)
	two := *resource.NewQuantity(2, resource.BinarySI)

	zone1Node1AvailableResources := &resources.Resources{
		CPU:    one,
		Memory: one,
	}
	zone1Node2FreeMemory := &resources.Resources{
		CPU:    one,
		Memory: two,
	}
	zone1Node3FreeCPU := &resources.Resources{
		CPU:    two,
		Memory: one,
	}
	zone2Node1AvailableResources := &resources.Resources{
		CPU:    one,
		Memory: one,
	}
	availableResources := resources.NodeGroupResources{
		zone1Node1.Name: zone1Node1AvailableResources,
		zone1Node2.Name: zone1Node2FreeMemory,
		zone1Node3.Name: zone1Node3FreeCPU,
		zone2Node1.Name: zone2Node1AvailableResources,
	}
	actualNodes := []*v1.Node{&zone1Node1, &zone1Node2, &zone1Node3, &zone2Node1}

	sortNodes(useExperimentalPriorities, actualNodes, availableResources)
	expectedResult := []*v1.Node{&zone2Node1, &zone1Node1, &zone1Node3, &zone1Node2}

	compareActualToExpected(actualNodes, expectedResult, t)
}

func TestAZAwareNodeSortingWorksIfZoneLabelIsMissing(t *testing.T) {
	node1 := getNode("node1")
	node2 := getNode("node2")
	node3 := getNode("node3")

	one := *resource.NewQuantity(1, resource.BinarySI)
	two := *resource.NewQuantity(2, resource.BinarySI)

	node1AvailableResources := &resources.Resources{
		CPU:    two,
		Memory: one,
	}
	node2AvailableResources := &resources.Resources{
		CPU:    one,
		Memory: two,
	}
	node3AvailableResources := &resources.Resources{
		CPU:    one,
		Memory: one,
	}
	availableResources := resources.NodeGroupResources{
		node1.Name: node1AvailableResources,
		node2.Name: node2AvailableResources,
		node3.Name: node3AvailableResources,
	}
	actualNodes := []*v1.Node{&node1, &node2, &node3}

	sortNodes(useExperimentalPriorities, actualNodes, availableResources)

	expectedResult := []*v1.Node{&node3, &node1, &node2}

	compareActualToExpected(actualNodes, expectedResult, t)
}

func compareActualToExpected(actualNodes []*v1.Node, expectedResult []*v1.Node, t *testing.T) {
	if len(actualNodes) != len(expectedResult) {
		t.Error("Length of nodes slice shouldn't change on sorting")
	}
	for i, expectedNode := range expectedResult {
		if expectedNode != actualNodes[i] {
			t.Error("Each element in the sorted result should match the expected result. Element unmatched: ", i)
		}
	}
}

func getNodeWithZoneLabel(name, zone string) v1.Node {
	return v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"failure-domain.beta.kubernetes.io/zone": zone,
			},
		},
	}
}

func getNode(name string) v1.Node {
	return v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
}
