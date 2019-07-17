package cache

import (
	"context"
	"github.com/palantir/k8s-spark-scheduler/internal/cache/queue"
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
	queue              queue.ModifiableQueue
	createCallback     func(metav1.Object, error)
	updateCallback     func(metav1.Object, error)
	deleteCallback     func(metav1.Object, error)
}

func NewAsyncClient(
	client rest.Interface,
	resourceName string,
	emptyObjectCreator func() object,
	createCallback func(metav1.Object, error),
	updateCallback func(metav1.Object, error),
	deleteCallback func(metav1.Object, error),
	queue queue.ModifiableQueue) *asyncClient {
	return &asyncClient{
		client:             client,
		resourceName:       resourceName,
		emptyObjectCreator: emptyObjectCreator,
		createCallback:     createCallback,
		updateCallback:     updateCallback,
		deleteCallback:     deleteCallback,
		queue:              queue,
	}
}

func (as *asyncClient) Run(ctx context.Context) {
	for i := 0; i < as.queue.Buckets(); i++ {
		go as.runWorker(ctx, i)
	}
}

func (as *asyncClient) runWorker(ctx context.Context, idx int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		r := as.queue.Get(idx)
		switch r.Type() {
		case queue.CreateRequestType:
			as.doCreate(ctx, r.Object())
		case queue.UpdateRequestType:
			as.doUpdate(ctx, r.Object())
		case queue.DeleteRequestType:
			as.doDelete(ctx, r.Object())
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
	as.createCallback(result, err) // TODO: if any update request is enqueued, update resource version
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
	as.updateCallback(result, err) // TODO: if any update request is enqueued, update resource version
}

func (as *asyncClient) doDelete(ctx context.Context, obj metav1.Object) {
	err := as.client.Delete().
		Namespace(obj.GetNamespace()).
		Resource(as.resourceName).
		Name(obj.GetName()).
		Do().
		Error()
	as.deleteCallback(obj, err)
}
