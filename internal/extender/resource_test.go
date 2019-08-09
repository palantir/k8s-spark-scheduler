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
	"testing"

	"github.com/palantir/k8s-spark-scheduler/internal/extender/extendertest"
)

func TestScheduler(t *testing.T) {
	node1 := extendertest.NewNode("node1")
	node2 := extendertest.NewNode("node2")
	nodeNames := []string{node1.Name, node2.Name}
	podsToSchedule := extendertest.SparkApplicationPods("2-executor-app", 2)

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
