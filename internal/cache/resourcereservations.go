package cache

import (
	"context"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	sparkschedulerclient "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler/internal/cache/store"
	clientcache "k8s.io/client-go/tools/cache"
)

// ResourceReservationCache is a cache for resource reservations
type ResourceReservationCache struct {
	cache       *cache
	asyncClient *asyncClient
}

// NewResourceReservationCache creates a new cache
func NewResourceReservationCache(
	resourceReservationInformer clientcache.SharedIndexInformer,
	resourceReservationClient sparkschedulerclient.SparkschedulerV1beta1Interface,
) *ResourceReservationCache {
	objectStore := store.NewStore()
	queue := store.NewShardedUniqueQueue(5)
	cache := newCache(queue, objectStore, resourceReservationInformer)
	asyncClient := &asyncClient{
		client:             resourceReservationClient.RESTClient(),
		resourceName:       v1beta1.ResourceReservationPlural,
		emptyObjectCreator: func() object { return &v1beta1.ResourceReservation{} },
		queue:              queue,
		objectStore:        objectStore,
	}
	return &ResourceReservationCache{
		cache:       cache,
		asyncClient: asyncClient,
	}
}

// Run starts the async clients of this cache
func (rrc *ResourceReservationCache) Run(ctx context.Context) {
	rrc.asyncClient.Run(ctx)
}

// Create enqueues a creation request and puts the object into the store
func (rrc *ResourceReservationCache) Create(rr *v1beta1.ResourceReservation) bool {
	return rrc.cache.Create(rr)
}

// Update enqueues an update request and updates the object in store
func (rrc *ResourceReservationCache) Update(rr *v1beta1.ResourceReservation) bool {
	return rrc.cache.Update(rr)
}

// Delete enqueues a deletion request and removes the object from store
func (rrc *ResourceReservationCache) Delete(rr *v1beta1.ResourceReservation) {
	rrc.cache.Delete(rr)
}

// Get returns the object from the store if it exists
func (rrc *ResourceReservationCache) Get(namespace, name string) (*v1beta1.ResourceReservation, bool) {
	obj, ok := rrc.cache.Get(namespace, name)
	return obj.(*v1beta1.ResourceReservation), ok
}

// List returns all known objects in the store
func (rrc *ResourceReservationCache) List() []*v1beta1.ResourceReservation {
	objects := rrc.cache.List()
	res := make([]*v1beta1.ResourceReservation, 0, len(objects))
	for _, o := range objects {
		res = append(res, o.(*v1beta1.ResourceReservation))
	}
	return res
}
