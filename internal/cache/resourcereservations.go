package cache

import (
	"context"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	sparkschedulerclient "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler/internal/cache/store"
	clientcache "k8s.io/client-go/tools/cache"
)

type ResourceReservationCache struct {
	cache       *cache
	asyncClient *asyncClient
}

func NewResourceReservationCache(
	resourceReservationInformer clientcache.SharedIndexInformer,
	resourceReservationClient sparkschedulerclient.SparkschedulerV1beta1Interface,
) *ResourceReservationCache {
	objectStore := store.NewStore()
	queue := store.NewShardedUniqueQueue(5)
	cache := NewCache(queue, objectStore, resourceReservationInformer)
	asyncClient := NewAsyncClient(
		resourceReservationClient.RESTClient(),
		v1beta1.ResourceReservationPlural,
		func() object { return &v1beta1.ResourceReservation{} },
		queue,
	)
	return &ResourceReservationCache{
		cache:       cache,
		asyncClient: asyncClient,
	}
}

func (rrc *ResourceReservationCache) Run(ctx context.Context) {
	rrc.asyncClient.Run(ctx)
}

func (rrc *ResourceReservationCache) Create(rr *v1beta1.ResourceReservation) bool {
	return rrc.cache.Create(rr)
}

func (rrc *ResourceReservationCache) Update(rr *v1beta1.ResourceReservation) bool {
	return rrc.cache.Update(rr)
}

func (rrc *ResourceReservationCache) Delete(rr *v1beta1.ResourceReservation) {
	rrc.cache.Delete(rr)
}

func (rrc *ResourceReservationCache) Get(namespace, name string) (*v1beta1.ResourceReservation, bool) {
	obj, ok := rrc.cache.Get(namespace, name)
	return obj.(*v1beta1.ResourceReservation), ok
}

func (rrc *ResourceReservationCache) List() []*v1beta1.ResourceReservation {
	objects := rrc.cache.List()
	res := make([]*v1beta1.ResourceReservation, 0, len(objects))
	for _, o := range objects {
		res = append(res, o.(*v1beta1.ResourceReservation))
	}
	return res
}
