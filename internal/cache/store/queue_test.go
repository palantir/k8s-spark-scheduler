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

package store

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
			{Key{"ns", "1"}, CreateRequestType},
			{Key{"ns", "2"}, UpdateRequestType},
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
			{Key{"ns", "2"}, UpdateRequestType},
			{Key{"ns", "1"}, CreateRequestType},
			{Key{"ns", "3"}, UpdateRequestType},
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
			{Key{"ns", "1"}, CreateRequestType},
			{Key{"ns", "1"}, DeleteRequestType},
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
