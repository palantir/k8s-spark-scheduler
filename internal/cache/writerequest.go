package cache

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type RequestType int

const (
	CreateRequestType RequestType = 0
	UpdateRequestType RequestType = 1
	DeleteRequestType RequestType = 2
)

type WriteRequest interface {
	GetType() RequestType
	SetObject(metav1.Object)
	Object() metav1.Object
}

func CreateRequest(obj metav1.Object) WriteRequest {
	return &request{obj, CreateRequestType}
}

func UpdateRequest(obj metav1.Object) WriteRequest {
	return &request{obj, UpdateRequestType}
}

func DeleteRequest(obj metav1.Object) WriteRequest {
	return &request{obj, DeleteRequestType}
}

type request struct {
	obj   metav1.Object
	_type RequestType
}

func (r *request) GetType() RequestType {
	return r._type
}

func (r *request) SetObject(obj metav1.Object) {
	r.obj = obj
}

func (r *request) Object() metav1.Object {
	return r.obj
}
