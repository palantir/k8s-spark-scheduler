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
	Key        Key
	Type       RequestType
	RetryCount int
}

// CreateRequest creates a create request for an object
func CreateRequest(obj metav1.Object) Request {
	return Request{KeyOf(obj), CreateRequestType, 0}
}

// UpdateRequest creates an update request for an object
func UpdateRequest(obj metav1.Object) Request {
	return Request{KeyOf(obj), UpdateRequestType, 0}
}

// DeleteRequest creates a delete request for an object
func DeleteRequest(objKey Key) Request {
	return Request{objKey, DeleteRequestType, 0}
}

// WithIncrementedRetryCount returns the same request with an incremented RetryCount
func (rq Request) WithIncrementedRetryCount() Request {
	return Request{rq.Key, rq.Type, rq.RetryCount + 1}
}
