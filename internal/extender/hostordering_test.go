package extender

import (
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
	"time"
)

func TestNodeSorting(t *testing.T) {
	var now = time.Unix(10000, 0)
	var zero = *resource.NewQuantity(0, resource.BinarySI)
	var one = *resource.NewQuantity(1, resource.BinarySI)
	var two = *resource.NewQuantity(2, resource.BinarySI)

	var oldest = scheduleContext{
		time.Unix(0, 0),
		zero,
		zero,
	}
	var older = scheduleContext{
		time.Unix(500, 0),
		zero,
		zero,
	}
	if compareNodes(oldest, older, now) || !compareNodes(older, oldest, now) {
		t.Error("Old nodes should be sorted youngest first")
	}
	var youngNode = scheduleContext{
		now,
		one,
		one,
	}
	if compareNodes(youngNode, oldest, now) || !compareNodes(oldest, youngNode, now) {
		t.Error("Old nodes should be sorted before young nodes")
	}
	var freeMemory = scheduleContext{
		now,
		two,
		zero,
	}
	if compareNodes(freeMemory, youngNode, now) || !compareNodes(youngNode, freeMemory, now) {
		t.Error("Young nodes should be sorted by how much memory is available ascending")
	}
	var freeCpu = scheduleContext{
		now,
		one,
		two,
	}
	if compareNodes(freeCpu, youngNode, now) || !compareNodes(youngNode, freeCpu, now) {
		t.Error("If used memory is equal, young nodes should be sorted by how much CPU is available ascending")
	}
	var allThingsEqual = scheduleContext{
		now.Add(time.Hour),
		one,
		one,
	}
	if compareNodes(allThingsEqual, youngNode, now) || !compareNodes(youngNode, allThingsEqual, now) {
		t.Error("If all other things are equal, we should prefer scheduling on the oldest young nodes")
	}
}
