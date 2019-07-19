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
			switch r.Type {
			case store.CreateRequestType:
				as.doCreate(ctx, r.Key)
			case store.UpdateRequestType:
				as.doUpdate(ctx, r.Key)
			case store.DeleteRequestType:
				as.doDelete(ctx, r.Key)
			}
		}
	}
}

func (as *asyncClient) doCreate(ctx context.Context, key store.Key) {
	obj := as.objectStore.Get(key.Namespace, key.Name)
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
	// TODO errors
}

func (as *asyncClient) doUpdate(ctx context.Context, key store.Key) {
	obj := as.objectStore.Get(key.Namespace, key.Name)
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
	// TODO errors
}

func (as *asyncClient) doDelete(ctx context.Context, key store.Key) {
	err := as.client.Delete().
		Namespace(key.Namespace).
		Resource(as.resourceName).
		Name(key.Name).
		Do().
		Error()
	if err != nil {
		as.objectStore.Delete(key.Namespace, key.Name)
	}
	// TODO errors
}
