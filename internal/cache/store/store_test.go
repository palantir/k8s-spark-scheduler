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
	"context"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func createObjectFromRV(name, namespace, resourceVersion string) metav1.Object {
	return &metav1.ObjectMeta{
		Name:            name,
		Namespace:       namespace,
		ResourceVersion: resourceVersion,
	}
}

func TestStore(t *testing.T) {
	tests := []struct {
		name            string
		body            func(context.Context, ObjectStore)
		expectedObjects map[string]metav1.Object
	}{{
		name: "Put does not override resourceVersion",
		body: func(ctx context.Context, s ObjectStore) {
			s.Put(createObjectFromRV("a", "a", ""))
			s.Put(createObjectFromRV("a", "a", "1"))
			s.Put(createObjectFromRV("b", "b", "2"))
			s.Put(createObjectFromRV("b", "b", ""))
		},
		expectedObjects: map[string]metav1.Object{
			"a": createObjectFromRV("a", "a", ""),
			"b": createObjectFromRV("b", "b", "2"),
		},
	}, {
		name: "PutIfAbsent does not override",
		body: func(ctx context.Context, s ObjectStore) {
			s.PutIfAbsent(createObjectFromRV("a", "a", ""))
			s.PutIfAbsent(createObjectFromRV("a", "a", "1"))
		},
		expectedObjects: map[string]metav1.Object{
			"a": createObjectFromRV("a", "a", ""),
		},
	}, {
		name: "PutIfNewer overrides if newer",
		body: func(ctx context.Context, s ObjectStore) {
			s.PutIfAbsent(createObjectFromRV("a", "a", ""))
			s.OverrideResourceVersionIfNewer(ctx, createObjectFromRV("a", "a", ""))
			s.OverrideResourceVersionIfNewer(ctx, createObjectFromRV("a", "a", "1"))
			s.OverrideResourceVersionIfNewer(ctx, createObjectFromRV("a", "a", "1"))
			s.OverrideResourceVersionIfNewer(ctx, createObjectFromRV("a", "a", "0"))
		},
		expectedObjects: map[string]metav1.Object{
			"a": createObjectFromRV("a", "a", "1"),
		},
	}, {
		name: "Delete ignores absent",
		body: func(ctx context.Context, s ObjectStore) {
			s.Delete(Key{"a", "a"})
			s.PutIfAbsent(createObjectFromRV("a", "a", "1"))
			s.Delete(Key{"a", "a"})
			s.PutIfAbsent(createObjectFromRV("a", "a", ""))
			s.OverrideResourceVersionIfNewer(ctx, createObjectFromRV("a", "a", "2"))
			s.PutIfAbsent(createObjectFromRV("b", "b", ""))
			s.Delete(Key{"b", "b"})
			s.Delete(Key{"c", "c"})
		},
		expectedObjects: map[string]metav1.Object{
			"a": createObjectFromRV("a", "a", "2"),
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := NewStore()
			test.body(context.Background(), s)
			actual := make(map[string]metav1.Object)
			for _, e := range s.List() {
				actual[e.GetName()] = e
			}
			if !reflect.DeepEqual(actual, test.expectedObjects) {
				t.Fatalf("expected:\n %v\n got:\n %v", test.expectedObjects, actual)
			}
		})
	}
}
