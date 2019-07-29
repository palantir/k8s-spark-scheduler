package store

import (
	"context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"
	"testing"
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
		name: "Put overrides",
		body: func(ctx context.Context, s ObjectStore) {
			s.Put(createObjectFromRV("a", "a", ""))
			s.Put(createObjectFromRV("a", "a", "1"))
			s.Put(createObjectFromRV("b", "b", "2"))
			s.Put(createObjectFromRV("b", "b", ""))
		},
		expectedObjects: map[string]metav1.Object{
			"a": createObjectFromRV("a", "a", "1"),
			"b": createObjectFromRV("b", "b", ""),
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
