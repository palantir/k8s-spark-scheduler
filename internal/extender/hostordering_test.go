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
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestNodeSorting(t *testing.T) {
	var one = *resource.NewQuantity(1, resource.BinarySI)
	var two = *resource.NewQuantity(2, resource.BinarySI)

	var node = resources.Resources{
		CPU:    one,
		Memory: one,
	}
	var freeMemory = resources.Resources{
		CPU:    one,
		Memory: two,
	}

	if lessThan(freeMemory, node) || !lessThan(node, freeMemory) {
		t.Error("Nodes should be sorted by how much memory is available ascending")
	}
	var freeCPU = resources.Resources{
		CPU:    two,
		Memory: one,
	}
	if lessThan(freeCPU, node) || !lessThan(node, freeCPU) {
		t.Error("If used memory is equal, nodes should be sorted by how much CPU is available ascending")
	}
}
