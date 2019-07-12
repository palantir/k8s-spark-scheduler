package cache

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

func TestModifiableQueue(t *testing.T) {
	queue := NewModifiableQueue(1)

	queue.AddOrUpdate(&request{createObject("1", "value1"), createRequest})
	queue.AddOrUpdate(&request{createObject("1", "value2"), updateRequest})

	actual := queue.Get(0)
	expected := &request{createObject("1", "value2"), createRequest}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("expected: %v, got: %v", expected, actual)
	}
}
