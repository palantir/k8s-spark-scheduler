package cache

import (
	"context"
	"github.com/palantir/k8s-spark-scheduler/internal/cache/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	rest "k8s.io/client-go/rest"
)

type object interface {
	metav1.Object
	runtime.Object
}

type asyncClient struct {
	client             rest.Interface
	resourceName       string
	emptyObjectCreator func() object
	queue              store.ShardedUniqueQueue
	objectStore        store.ObjectStore
}

func NewAsyncClient(
	client rest.Interface,
	resourceName string,
	emptyObjectCreator func() object,
	queue store.ShardedUniqueQueue) *asyncClient {
	return &asyncClient{
		client:             client,
		resourceName:       resourceName,
		emptyObjectCreator: emptyObjectCreator,
		queue:              queue,
	}
}

func (as *asyncClient) Run(ctx context.Context) {
	for _, q := range as.queue.GetConsumers() {
		go as.runWorker(ctx, q)
	}
}

func (as *asyncClient) runWorker(ctx context.Context, requests <-chan func() store.Request) {
	for {
		select {
		case <-ctx.Done():
			return
		case requestGetter := <-requests:
			r := requestGetter()
			obj := as.objectStore.Get(r.Key.Namespace, r.Key.Name)
			switch r.Type {
			case store.CreateRequest:
				as.doCreate(ctx, obj)
			case store.UpdateRequest:
				as.doUpdate(ctx, obj)
			case store.DeleteRequest:
				as.doDelete(ctx, obj)
			}
		}
	}
}

func (as *asyncClient) doCreate(ctx context.Context, obj metav1.Object) {
	result := as.emptyObjectCreator()
	err := as.client.Post().
		Namespace(obj.GetNamespace()).
		Resource(as.resourceName).
		Body(obj).
		Do().
		Into(result)
	if err != nil {
		as.objectStore.PutIfNewer(obj)
	}
	//as.createCallback(result, err) // TODO: if any update request is enqueued, update resource version
}

func (as *asyncClient) doUpdate(ctx context.Context, obj metav1.Object) {
	result := as.emptyObjectCreator()
	err := as.client.Put().
		Namespace(obj.GetNamespace()).
		Resource(as.resourceName).
		Name(obj.GetName()).
		Body(obj).
		Do().
		Into(result)
	if err != nil {
		as.objectStore.PutIfNewer(obj)
	}
	//as.updateCallback(result, err) // TODO: if any update request is enqueued, update resource version
}

func (as *asyncClient) doDelete(ctx context.Context, obj metav1.Object) {
	err := as.client.Delete().
		Namespace(obj.GetNamespace()).
		Resource(as.resourceName).
		Name(obj.GetName()).
		Do().
		Error()
	if err != nil {
		as.objectStore.Delete(obj.GetNamespace(), obj.GetName())
	}
	//as.deleteCallback(obj, err)
}
