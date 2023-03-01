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

package extender_test

import (
	"fmt"
	"testing"

	"github.com/palantir/k8s-spark-scheduler/internal/extender/extendertest"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestScheduler(t *testing.T) {
	node1 := extendertest.NewNode("node1")
	node2 := extendertest.NewNode("node2")
	nodeNames := []string{node1.Name, node2.Name}
	podsToSchedule := extendertest.StaticAllocationSparkPods("2-executor-app", 2)

	testHarness, err := extendertest.NewTestExtender(
		&node1,
		&node2,
		&podsToSchedule[0],
		&podsToSchedule[1],
		&podsToSchedule[2],
	)
	if err != nil {
		t.Fatal("Could not setup test extender")
	}

	for _, pod := range podsToSchedule {
		testHarness.AssertSuccessfulSchedule(
			t,
			pod,
			nodeNames,
			"There should be enough capacity to schedule the full application")
	}

	newExecutor := podsToSchedule[1]
	newExecutor.Name = "newly-requested-exec"
	testHarness.AssertFailedSchedule(
		t,
		newExecutor,
		nodeNames,
		"Since all reservations are bound, a new executor should not be scheduled")

	err = testHarness.TerminatePod(podsToSchedule[1])
	if err != nil {
		t.Fatal("Could not terminate pod in test extender")
	}

	testHarness.AssertSuccessfulSchedule(
		t,
		newExecutor,
		nodeNames,
		"Because an executor is terminated, the new request can replace its reservation")
}

func TestDynamicAllocationScheduling(t *testing.T) {
	tests := []struct {
		name                                 string
		podsToSchedule                       []v1.Pod
		scenario                             func(harness *extendertest.Harness, podsToSchedule []v1.Pod, nodeNames []string)
		expectedReservations                 []string
		expectedPodToNodeSoftReservationsMap map[string]string
	}{{
		name:           "creates a reservation when under min executor count",
		podsToSchedule: extendertest.DynamicAllocationSparkPods("dynamic-allocation-app", 1, 3),
		scenario: func(harness *extendertest.Harness, podsToSchedule []v1.Pod, nodeNames []string) {
			harness.Schedule(t, podsToSchedule[0], nodeNames)
			harness.Schedule(t, podsToSchedule[1], nodeNames)
		},
		expectedReservations:                 []string{executor(0)},
		expectedPodToNodeSoftReservationsMap: map[string]string{},
	}, {
		name:           "creates a soft reservation for an executor over min executor count",
		podsToSchedule: extendertest.DynamicAllocationSparkPods("dynamic-allocation-app", 1, 3),
		scenario: func(harness *extendertest.Harness, podsToSchedule []v1.Pod, nodeNames []string) {
			harness.Schedule(t, podsToSchedule[0], nodeNames)
			harness.Schedule(t, podsToSchedule[1], nodeNames)
			harness.Schedule(t, podsToSchedule[2], nodeNames)
		},
		expectedReservations: []string{executor(0)},
		expectedPodToNodeSoftReservationsMap: map[string]string{
			executor(1): "node1",
		},
	}, {
		name:           "soft reservations are created on full nodes first",
		podsToSchedule: extendertest.DynamicAllocationSparkPods("dynamic-allocation-app", 1, 2),
		scenario: func(harness *extendertest.Harness, podsToSchedule []v1.Pod, nodeNames []string) {
			harness.Schedule(t, podsToSchedule[0], nodeNames[1:])
			harness.Schedule(t, podsToSchedule[1], nodeNames[1:])
			harness.Schedule(t, podsToSchedule[2], nodeNames)
		},
		expectedReservations: []string{executor(0)},
		expectedPodToNodeSoftReservationsMap: map[string]string{
			executor(1): "node2",
		},
	}, {
		name:           "does not create any reservation for an executor over the max",
		podsToSchedule: extendertest.DynamicAllocationSparkPods("dynamic-allocation-app", 1, 3),
		scenario: func(harness *extendertest.Harness, podsToSchedule []v1.Pod, nodeNames []string) {
			for _, pod := range podsToSchedule {
				harness.Schedule(t, pod, nodeNames)
			}
			harness.Schedule(t, podsToSchedule[3], nodeNames) // should not have any reservation
		},
		expectedReservations: []string{executor(0)},
		expectedPodToNodeSoftReservationsMap: map[string]string{
			executor(1): "node1",
			executor(2): "node1",
		},
	}, {
		name:           "replaces a dead executor's resource reservation before adding a new soft reservation",
		podsToSchedule: extendertest.DynamicAllocationSparkPods("dynamic-allocation-app", 1, 3),
		scenario: func(harness *extendertest.Harness, podsToSchedule []v1.Pod, nodeNames []string) {
			harness.Schedule(t, podsToSchedule[0], nodeNames) // driver
			harness.Schedule(t, podsToSchedule[1], nodeNames) // executor-0 with a resource reservation
			harness.Schedule(t, podsToSchedule[2], nodeNames) // executor-1 with a soft reservation
			// kill executor-0
			if err := harness.TerminatePod(podsToSchedule[1]); err != nil {
				t.Fatal("Could not terminate pod in test extender")
			}
			harness.Schedule(t, podsToSchedule[3], nodeNames) // executor-2 should have a resource reservation
		},
		expectedReservations: []string{executor(2)},
		expectedPodToNodeSoftReservationsMap: map[string]string{
			executor(1): "node1",
		},
	},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node1 := extendertest.NewNode("node1")
			node2 := extendertest.NewNode("node2")
			nodeNames := []string{node1.Name, node2.Name}
			harnessArgs := make([]runtime.Object, 0, len(test.podsToSchedule)+2)
			harnessArgs = append(harnessArgs, &node1, &node2)
			for i := range test.podsToSchedule {
				harnessArgs = append(harnessArgs, &test.podsToSchedule[i])
			}
			testHarness, err := extendertest.NewTestExtender(harnessArgs...)
			if err != nil {
				t.Fatal("Could not setup test extender")
			}

			test.scenario(testHarness, test.podsToSchedule, nodeNames)

			// Compare expected and actual resource reservations
			expectedExecutorReservations := make(map[string]bool)
			for _, expectedRes := range test.expectedReservations {
				expectedExecutorReservations[expectedRes] = true
			}
			extraExecutors := make(map[string]bool)
			for _, resourceReservation := range testHarness.ResourceReservationCache.List() {
				for name, podName := range resourceReservation.Status.Pods {
					if name != "driver" {
						if _, exists := expectedExecutorReservations[podName]; exists {
							delete(expectedExecutorReservations, podName)
						} else {
							extraExecutors[podName] = true
						}
					}
				}
			}

			if len(expectedExecutorReservations) > 0 {
				t.Errorf("expected the following executors to have reservations, but did not: %v", expectedExecutorReservations)
			}
			if len(extraExecutors) > 0 {
				t.Errorf("following executors had reservations, but were not supposed to: %v", extraExecutors)
			}

			// Compare expected and actual soft reservations
			expectedSoftReservationPodToNode := make(map[string]string)
			for podName, nodeName := range test.expectedPodToNodeSoftReservationsMap {
				expectedSoftReservationPodToNode[podName] = nodeName
			}
			unexpectedSoftReservationPodToNode := make(map[string]string)

			for _, actualSoftReservations := range testHarness.SoftReservationStore.GetAllSoftReservationsCopy() {
				for actualPodName, actualSoftReservation := range actualSoftReservations.Reservations {
					if expectedNodeName, ok := expectedSoftReservationPodToNode[actualPodName]; ok {
						if expectedNodeName == actualSoftReservation.Node {
							delete(expectedSoftReservationPodToNode, actualPodName)
						} else {
							// we expected an actualSoftReservation, but for a different node
							unexpectedSoftReservationPodToNode[actualPodName] = actualSoftReservation.Node
						}
					} else {
						unexpectedSoftReservationPodToNode[actualPodName] = actualSoftReservation.Node
					}
				}
			}

			if len(expectedSoftReservationPodToNode) > 0 {
				t.Errorf("expected the following executors to have soft reservations, but did not: %v", expectedSoftReservationPodToNode)
			}
			if len(unexpectedSoftReservationPodToNode) > 0 {
				t.Errorf("following executors had soft reservations, but were not supposed to: %v", unexpectedSoftReservationPodToNode)
			}
		})
	}
}

func executor(i int) string {
	return fmt.Sprintf("spark-exec-%d", i)
}
