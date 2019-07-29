package cache

import (
	"context"
	"github.com/palantir/k8s-spark-scheduler/internal/cache/store"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientcache "k8s.io/client-go/tools/cache"
)

type cache struct {
	store store.ObjectStore
	queue store.ShardedUniqueQueue
}

func NewCache(
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
	return c.store.Get(store.Key{namespace, name})
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
