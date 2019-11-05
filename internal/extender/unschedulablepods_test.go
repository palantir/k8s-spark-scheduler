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

func TestUnschedulablePodMarker(t *testing.T) {
	node1 := extendertest.NewNode("node1")
	node2 := extendertest.NewNode("node2")

	testHarness, err := extendertest.NewTestExtender(
		&node1,
		&node2)
	if err != nil {
		t.Fatal("Could not setup test extender")
	}

	twoExecutorsDriver := extendertest.StaticAllocationSparkPods("2-executor-app", 2)[0]
	doesExceed, err := testHarness.UnschedulablePodMarker.DoesPodExceedClusterCapacity(testHarness.Ctx, &twoExecutorsDriver)
	if err != nil {
		t.Errorf("exceeds capacity check should not cause an error: %s", err)
	}
	if doesExceed {
		t.Error("The two executor application should fit to the cluster")
	}

	hundredExecutorsDriver := extendertest.StaticAllocationSparkPods("100-executor-app", 100)[0]
	doesExceed, err = testHarness.UnschedulablePodMarker.DoesPodExceedClusterCapacity(testHarness.Ctx, &hundredExecutorsDriver)
	if err != nil {
		t.Errorf("exceeds capacity check should not cause an error: %s", err)
	}
	if !doesExceed {
		t.Error("The hundred executor application should not fit to the cluster")
	}
}
