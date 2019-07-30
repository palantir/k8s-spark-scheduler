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

package cache

import (
	"context"

	"github.com/palantir/k8s-spark-scheduler/internal/cache/store"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientcache "k8s.io/client-go/tools/cache"
)

// cache manages a store of object, and reflects
// in flight write requests as they are completed
// successfully. It ignores external updates and external
// creates to avoid conflicts. Users of this need to be
// the only writing entity for the cached elements.
type cache struct {
	store store.ObjectStore
	queue store.ShardedUniqueQueue
}

func newCache(
	queue store.ShardedUniqueQueue,
	store store.ObjectStore,
	informer clientcache.SharedIndexInformer) *cache {
	c := &cache{
		queue: queue,
		store: store,
	}
	informer.AddEventHandler(
		clientcache.ResourceEventHandlerFuncs{
			AddFunc:    c.onObjAdd,
			UpdateFunc: c.onObjUpdate,
			DeleteFunc: c.onObjDelete,
		},
	)
	return c
}

func (c *cache) Create(obj metav1.Object) bool {
	created := c.store.PutIfAbsent(obj)
	if !created {
		return false
	}
	c.queue.AddIfAbsent(store.CreateRequest(obj))
	return true
}

func (c *cache) Get(namespace, name string) (metav1.Object, bool) {
	return c.store.Get(store.Key{Namespace: namespace, Name: name})
}

func (c *cache) Update(obj metav1.Object) bool {
	_, ok := c.store.Get(store.KeyOf(obj))
	if !ok {
		return false
	}
	c.store.Put(obj)
	c.queue.AddIfAbsent(store.UpdateRequest(obj))
	return true
}

func (c *cache) Delete(obj metav1.Object) {
	c.store.Delete(store.KeyOf(obj))
	c.queue.AddIfAbsent(store.DeleteRequest(obj))
}

func (c *cache) List() []metav1.Object {
	return c.store.List()
}

func (c *cache) onObjAdd(obj interface{}) {
	ctx := context.Background()
	typedObject, ok := obj.(metav1.Object)
	if !ok {
		svc1log.FromContext(ctx).Warn("failed to parse object")
		return
	}
	c.store.OverrideResourceVersionIfNewer(ctx, typedObject)
}

func (c *cache) onObjUpdate(oldObj interface{}, newObj interface{}) {
	ctx := context.Background()
	typedObject, ok := newObj.(metav1.Object)
	if !ok {
		svc1log.FromContext(ctx).Warn("failed to parse object")
		return
	}
	c.store.OverrideResourceVersionIfNewer(ctx, typedObject)
}

func (c *cache) onObjDelete(obj interface{}) {
	typedObject, ok := obj.(metav1.Object)
	if !ok {
		svc1log.FromContext(context.Background()).Warn("failed to parse object")
		return
	}
	c.store.Delete(store.KeyOf(typedObject))
}
