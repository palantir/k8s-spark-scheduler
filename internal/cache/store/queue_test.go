package store

import (
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
		queue            ShardedUniqueQueue
		body             func(q ShardedUniqueQueue)
		expectedElements []Request
	}{{
		name:  "updates enqueued elements",
		queue: NewShardedUniqueQueue(1),
		body: func(q ShardedUniqueQueue) {
			q.AddIfAbsent(Request{Key{"ns", "1"}, CreateRequestType})
			q.AddIfAbsent(Request{Key{"ns", "1"}, UpdateRequestType})
			q.AddIfAbsent(Request{Key{"ns", "2"}, UpdateRequestType})
			q.AddIfAbsent(Request{Key{"ns", "2"}, UpdateRequestType})
		},
		expectedElements: []Request{
			Request{Key{"ns", "1"}, CreateRequestType},
			Request{Key{"ns", "2"}, UpdateRequestType},
		},
	}, {
		name:  "updates enqueued elements on partitioned queues",
		queue: NewShardedUniqueQueue(10),
		body: func(q ShardedUniqueQueue) {
			q.AddIfAbsent(Request{Key{"ns", "1"}, CreateRequestType})
			q.AddIfAbsent(Request{Key{"ns", "2"}, UpdateRequestType})
			q.AddIfAbsent(Request{Key{"ns", "3"}, UpdateRequestType})
			q.AddIfAbsent(Request{Key{"ns", "3"}, UpdateRequestType})
		},
		expectedElements: []Request{
			Request{Key{"ns", "2"}, UpdateRequestType},
			Request{Key{"ns", "1"}, CreateRequestType},
			Request{Key{"ns", "3"}, UpdateRequestType},
		},
	}, {
		name:  "deletions are enqueued even if their key is present",
		queue: NewShardedUniqueQueue(1),
		body: func(q ShardedUniqueQueue) {
			q.AddIfAbsent(Request{Key{"ns", "1"}, CreateRequestType})
			q.AddIfAbsent(Request{Key{"ns", "1"}, UpdateRequestType})
			q.AddIfAbsent(Request{Key{"ns", "1"}, DeleteRequestType})
		},
		expectedElements: []Request{
			Request{Key{"ns", "1"}, CreateRequestType},
			Request{Key{"ns", "1"}, DeleteRequestType},
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.body(test.queue)
			actual := make([]Request, 0, len(test.expectedElements))
			for _, q := range test.queue.GetConsumers() {
				for len(q) > 0 {
					r := (<-q)()
					actual = append(actual, r)
				}
			}
			if !reflect.DeepEqual(actual, test.expectedElements) {
				t.Fatalf("expected:\n %v\n got:\n %v", test.expectedElements, actual)
			}
		})
	}
}
