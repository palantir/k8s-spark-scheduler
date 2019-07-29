package store

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Key is used to identify objects in store
type Key struct {
	Namespace string
	Name      string
}

// KeyOf creates a key from an object
func KeyOf(obj metav1.Object) Key {
	return Key{obj.GetNamespace(), obj.GetName()}
}

// RequestType denotes the type of write request
type RequestType int

const (
	// CreateRequestType denotes create requests
	CreateRequestType RequestType = 0
	// UpdateRequestType denotes update requests
	UpdateRequestType RequestType = 1
	// DeleteRequestType denotes delete requests
	DeleteRequestType RequestType = 2
)

// Request is a write request for an object
type Request struct {
	Key  Key
	Type RequestType
}

// CreateRequest creates a create request for an object
func CreateRequest(obj metav1.Object) Request {
	return Request{KeyOf(obj), CreateRequestType}
}

// UpdateRequest creates an update request for an object
func UpdateRequest(obj metav1.Object) Request {
	return Request{KeyOf(obj), UpdateRequestType}
}

// DeleteRequest creates a delete request for an object
func DeleteRequest(obj metav1.Object) Request {
	return Request{KeyOf(obj), DeleteRequestType}
}

// KeySafeParams returns safe logging params for a key object
func KeySafeParams(k Key) map[string]interface{} {
	return map[string]interface{}{
		"objectName":      k.Name,
		"objectNamespace": k.Namespace,
	}
}
