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
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
)

func TestNodeSorting(t *testing.T) {
	var now = time.Unix(0, 0)
	var zero = *resource.NewQuantity(0, resource.BinarySI)
	var one = *resource.NewQuantity(1, resource.BinarySI)
	var two = *resource.NewQuantity(2, resource.BinarySI)

	var node = scheduleContext{
		now,
		one,
		one,
	}
	var freeMemory = scheduleContext{
		now,
		two,
		zero,
	}
	if compareNodes(freeMemory, node, now) || !compareNodes(node, freeMemory, now) {
		t.Error("Young nodes should be sorted by how much memory is available ascending")
	}
	var freeCPU = scheduleContext{
		now,
		one,
		two,
	}
	if compareNodes(freeCPU, node, now) || !compareNodes(node, freeCPU, now) {
		t.Error("If used memory is equal, young nodes should be sorted by how much CPU is available ascending")
	}
	var allThingsEqual = scheduleContext{
		now.Add(time.Hour),
		one,
		one,
	}
	if compareNodes(allThingsEqual, node, now) || !compareNodes(node, allThingsEqual, now) {
		t.Error("If all other things are equal, we should prefer scheduling on the oldest young nodes")
	}
}
