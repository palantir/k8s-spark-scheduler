package cache

import (
	"encoding/json"
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
		queue            *modifiableQueue
		body             func(q *modifiableQueue)
		expectedElements map[types.UID]WriteRequest
	}{{
		name:  "updates enqueued elements",
		queue: NewModifiableQueue(1),
		body: func(q *modifiableQueue) {
			q.AddOrUpdate(CreateRequest(createObject("1", "value1")))
			q.AddOrUpdate(UpdateRequest(createObject("1", "value2")))
			q.Get(0)
			q.AddOrUpdate(UpdateRequest(createObject("2", "value1")))
			q.AddOrUpdate(UpdateRequest(createObject("1", "value3")))
			q.AddOrUpdate(UpdateRequest(createObject("2", "value2")))
		},
		expectedElements: map[types.UID]WriteRequest{
			types.UID("1"): UpdateRequest(createObject("1", "value3")),
			types.UID("2"): UpdateRequest(createObject("2", "value2")),
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.body(test.queue)
			actual := make(map[types.UID]WriteRequest)
			for i := 0; i < len(test.expectedElements); i++ {
				r := test.queue.Get(0)
				actual[r.Object().GetUID()] = r
			}
			if !reflect.DeepEqual(actual, test.expectedElements) {
				expectedJson, _ := json.Marshal(test.expectedElements)
				actualJson, _ := json.Marshal(actual)
				t.Fatalf("expected:\n %v\n got:\n %v", string(expectedJson), string(actualJson))
			}
		})
	}
}
