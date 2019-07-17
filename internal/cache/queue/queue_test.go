package queue

import (
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"reflect"
	"testing"
)

func createObject(uid string, labelValue string) metav1.Object {
	return &metav1.ObjectMeta{
		UID:    types.UID(uid),
		Labels: map[string]string{"test": labelValue},
	}
}

func TestQueue(t *testing.T) {
	tests := []struct {
		name             string
		queue            ModifiableQueue
		body             func(q ModifiableQueue)
		expectedElements map[types.UID]WriteRequest
	}{{
		name:  "updates enqueued elements",
		queue: NewModifiableQueue(1),
		body: func(q ModifiableQueue) {
			q.AddOrUpdate(CreateRequest(createObject("1", "value1")))
			q.AddOrUpdate(UpdateRequest(createObject("1", "value2")))
			(<-q.GetConsumers()[0])() // Consume the object with uid 1
			q.AddOrUpdate(UpdateRequest(createObject("2", "value1")))
			q.AddOrUpdate(UpdateRequest(createObject("1", "value3")))
			q.AddOrUpdate(UpdateRequest(createObject("2", "value2")))
		},
		expectedElements: map[types.UID]WriteRequest{
			types.UID("1"): UpdateRequest(createObject("1", "value3")),
			types.UID("2"): UpdateRequest(createObject("2", "value2")),
		},
	}, {
		name:  "updates enqueued elements on partitioned queues",
		queue: NewModifiableQueue(10),
		body: func(q ModifiableQueue) {
			q.AddOrUpdate(CreateRequest(createObject("1", "value1")))
			q.AddOrUpdate(UpdateRequest(createObject("2", "value1")))
			q.AddOrUpdate(CreateRequest(createObject("3", "value1")))
			q.AddOrUpdate(UpdateRequest(createObject("3", "value2")))
		},
		expectedElements: map[types.UID]WriteRequest{
			types.UID("1"): CreateRequest(createObject("1", "value1")),
			types.UID("2"): UpdateRequest(createObject("2", "value1")),
			types.UID("3"): CreateRequest(createObject("3", "value2")),
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.body(test.queue)
			actual := make(map[types.UID]WriteRequest)
			for _, q := range test.queue.GetConsumers() {
				for len(q) > 0 {
					r := (<-q)()
					actual[r.Object().GetUID()] = r
				}
			}
			if !reflect.DeepEqual(actual, test.expectedElements) {
				t.Fatalf("expected:\n %v\n got:\n %v", format(test.expectedElements), format(actual))
			}
		})
	}
}

func format(res map[types.UID]WriteRequest) string {
	str := ""
	for k, v := range res {
		str += fmt.Sprintf("uid: %s -> type: %d uid: %s labels: %s\n",
			k, v.Type(), v.Object().GetUID(), v.Object().GetLabels())
	}
	return str
}
