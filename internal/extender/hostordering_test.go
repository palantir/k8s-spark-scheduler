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

	v1 "k8s.io/api/core/v1"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNodeSorting(t *testing.T) {
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

	if lessThan(freeMemory, node) || !lessThan(node, freeMemory) {
		t.Error("Nodes should be sorted by how much memory is available ascending")
	}
	var freeCPU = &resources.Resources{
		CPU:    two,
		Memory: one,
	}
	if lessThan(freeCPU, node) || !lessThan(node, freeCPU) {
		t.Error("If used memory is equal, nodes should be sorted by how much CPU is available ascending")
	}
}

func TestAZAwareScheduling(t *testing.T) {
	zone1Node1 := NewNode("zone1Node1", "zone1")
	zone1Node2 := NewNode("zone1Node2", "zone1")
	zone1Node3 := NewNode("zone1Node3", "zone1")
	zone2Node1 := NewNode("zone2Node1", "zone2")

	var one = *resource.NewQuantity(1, resource.BinarySI)
	var two = *resource.NewQuantity(2, resource.BinarySI)

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
	var mockExtender = NewExtender(
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		true,
		nil,
		nil,
		"something",
		true)
	mockExtender.sortNodes(actualNodes, availableResources)

	expectedResult := []*v1.Node{&zone2Node1, &zone1Node1, &zone1Node3, &zone1Node2}
	if len(actualNodes) != len(expectedResult) {
		t.Error("Length of nodes slice shouldn't change on sorting")
	}
	for i, expectedNode := range expectedResult {
		if expectedNode != actualNodes[i] {
			t.Error("Each element in the sorted result should match the expected result. Element unmatched: ", i)
		}
	}
}

func NewNode(name, zone string) v1.Node {
	return v1.Node{
		TypeMeta: metav1.TypeMeta{
			Kind: "node",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"failure-domain.beta.kubernetes.io/zone": zone,
				"resource_channel":                       "batch-medium-priority",
				"com.palantir.rubix/instance-group":      "batch-medium-priority",
				"test":                                   "something",
			},
			Annotations: map[string]string{},
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(8, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewQuantity(8*1024*1024*1024, resource.BinarySI),
			},
		},
	}
}
