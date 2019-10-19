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
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
)

func TestNodeSorting(t *testing.T) {
	var zero = *resource.NewQuantity(0, resource.BinarySI)
	var one = *resource.NewQuantity(1, resource.BinarySI)
	var two = *resource.NewQuantity(2, resource.BinarySI)

	var node = scheduleContext{
		one,
		one,
	}
	var freeMemory = scheduleContext{
		two,
		zero,
	}

	if compareNodes(freeMemory, node) || !compareNodes(node, freeMemory) {
		t.Error("Young nodes should be sorted by how much memory is available ascending")
	}
	var freeCPU = scheduleContext{
		one,
		two,
	}
	if compareNodes(freeCPU, node) || !compareNodes(node, freeCPU) {
		t.Error("If used memory is equal, young nodes should be sorted by how much CPU is available ascending")
	}
}
