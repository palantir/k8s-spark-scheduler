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
	"strconv"
	"sync"

	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ObjectStore is a thread safe store for kubernetes resources
type ObjectStore interface {
	Put(metav1.Object)
	OverrideResourceVersionIfNewer(context.Context, metav1.Object) bool
	PutIfAbsent(metav1.Object) bool
	Get(Key) (metav1.Object, bool)
	Delete(Key)
	List() []metav1.Object
}

type objectStore struct {
	store map[Key]metav1.Object
	lock  sync.RWMutex
}

// NewStore creates an empty store
func NewStore() ObjectStore {
	return &objectStore{
		store: make(map[Key]metav1.Object),
	}
}

func (s *objectStore) Put(obj metav1.Object) {
	s.lock.Lock()
	defer s.lock.Unlock()
	key := KeyOf(obj)
	currentObj, ok := s.store[key]
	if ok {
		obj.SetResourceVersion(currentObj.GetResourceVersion())
	}
	s.store[KeyOf(obj)] = obj
}

func (s *objectStore) OverrideResourceVersionIfNewer(ctx context.Context, obj metav1.Object) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	key := KeyOf(obj)
	currentObj, ok := s.store[key]
	if ok && resourceVersion(ctx, currentObj) >= resourceVersion(ctx, obj) {
		return false
	}
	currentObj.SetResourceVersion(obj.GetResourceVersion())
	return true
}

func (s *objectStore) PutIfAbsent(obj metav1.Object) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	k := KeyOf(obj)
	_, ok := s.store[k]
	if ok {
		return false
	}
	s.store[k] = obj
	return true
}

func (s *objectStore) Get(key Key) (metav1.Object, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	obj, exists := s.store[key]
	return obj, exists
}

func (s *objectStore) Delete(key Key) {
	_, ok := s.Get(key)
	if !ok {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.store, key)
}

func (s *objectStore) List() []metav1.Object {
	s.lock.RLock()
	defer s.lock.RUnlock()
	res := make([]metav1.Object, 0, len(s.store))
	for _, o := range s.store {
		res = append(res, o)
	}
	return res
}

func resourceVersion(ctx context.Context, obj metav1.Object) uint64 {
	rv := obj.GetResourceVersion()
	if len(rv) == 0 {
		return 0
	}
	version, err := strconv.ParseUint(rv, 10, 64)
	if err != nil {
		svc1log.FromContext(ctx).Error("failed to parse resourceVersion, using 0",
			svc1log.SafeParam("objectNamespace", obj.GetNamespace()),
			svc1log.SafeParam("objectName", obj.GetNamespace()),
			svc1log.Stacktrace(err))
		return 0
	}
	return version
}
