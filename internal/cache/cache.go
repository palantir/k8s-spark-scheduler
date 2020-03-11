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
	werror "github.com/palantir/witchcraft-go-error"
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
	store  store.ObjectStore
	queue  store.ShardedUniqueQueue
	logger svc1log.Logger
}

func newCache(
	ctx context.Context,
	queue store.ShardedUniqueQueue,
	store store.ObjectStore,
	informer clientcache.SharedIndexInformer) *cache {
	c := &cache{
		queue:  queue,
		store:  store,
		logger: svc1log.FromContext(ctx),
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

func (c *cache) Create(obj metav1.Object) error {
	created := c.store.PutIfAbsent(obj)
	if !created {
		return werror.Error("object already exists")
	}
	c.queue.AddIfAbsent(store.CreateRequest(obj))
	return nil
}

func (c *cache) Get(namespace, name string) (metav1.Object, bool) {
	return c.store.Get(store.Key{Namespace: namespace, Name: name})
}

func (c *cache) Update(obj metav1.Object) error {
	_, ok := c.store.Get(store.KeyOf(obj))
	if !ok {
		return werror.Error("object does not exist")
	}
	c.store.Put(obj)
	c.queue.AddIfAbsent(store.UpdateRequest(obj))
	return nil
}

func (c *cache) Delete(namespace, name string) {
	key := store.Key{Namespace: namespace, Name: name}
	c.store.Delete(key)
	c.queue.AddIfAbsent(store.DeleteRequest(key))
}

func (c *cache) List() []metav1.Object {
	return c.store.List()
}

func (c *cache) onObjAdd(obj interface{}) {
	c.tryOverrideResourceVersion(obj)
}

func (c *cache) onObjUpdate(oldObj interface{}, newObj interface{}) {
	c.tryOverrideResourceVersion(newObj)
}

func (c *cache) onObjDelete(obj interface{}) {
	typedObject, ok := obj.(metav1.Object)
	if !ok {
		c.logger.Warn("failed to parse object, trying to get from tombstone")
		tombstone, ok := obj.(clientcache.DeletedFinalStateUnknown)
		if !ok {
			c.logger.Error("failed to get tombstone for object")
			return
		}
		typedObject, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			c.logger.Error("failed to get object from tombstone")
			return
		}
	}
	c.logger.Info("received deletion event", ObjectSafeParams(typedObject.GetName(), typedObject.GetNamespace()))
	c.store.Delete(store.KeyOf(typedObject))
}

func (c *cache) tryOverrideResourceVersion(obj interface{}) {
	typedObject, ok := obj.(metav1.Object)
	if !ok {
		c.logger.Warn("failed to parse object")
		return
	}
	c.store.OverrideResourceVersionIfNewer(typedObject)
}

// ObjectSafeParams returns safe logging params for a name and a namespace
func ObjectSafeParams(name, namespace string) svc1log.Param {
	return svc1log.SafeParams(map[string]interface{}{
		"objectName":      name,
		"objectNamespace": namespace,
	})
}
