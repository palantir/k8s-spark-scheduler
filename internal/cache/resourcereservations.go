package cache

import (
	"context"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	sparkschedulerclient "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/sparkscheduler/v1beta1"
	rrinformers "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/informers/externalversions/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler/internal/cache/store"
	"k8s.io/apimachinery/pkg/labels"
)

// ResourceReservationCache is a cache for resource reservations.
// It assumes it is the only client that issues write requests for
// resource reservations. Any external update and creation will be
// ignored, but deletions will be reflected in the cache.
type ResourceReservationCache struct {
	cache       *cache
	asyncClient *asyncClient
}

// NewResourceReservationCache creates a new cache.
func NewResourceReservationCache(
	resourceReservationInformer rrinformers.ResourceReservationInformer,
	resourceReservationClient sparkschedulerclient.SparkschedulerV1beta1Interface,
) (*ResourceReservationCache, error) {
	rrs, err := resourceReservationInformer.Lister().List(labels.Everything())
	if err != nil {
		return nil, err
	}
	objectStore := store.NewStore()
	for _, rr := range rrs {
		objectStore.Put(rr)
	}
	queue := store.NewShardedUniqueQueue(5)
	cache := newCache(queue, objectStore, resourceReservationInformer.Informer())
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
	}, nil
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
