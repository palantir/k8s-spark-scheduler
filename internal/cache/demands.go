package cache

import (
	"context"
	demandapi "github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha1"
	demandclient "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/scaler/v1alpha1"
	"github.com/palantir/k8s-spark-scheduler/internal/cache/store"
	clientcache "k8s.io/client-go/tools/cache"
)

// DemandCache is a cache for demands. It assumes it is the only
// client that creates demands. Externally created demands will not be
// included in the cache.
type DemandCache struct {
	cache       *cache
	asyncClient *asyncClient
}

// NewDemandCache creates a new cache
func NewDemandCache(
	demandInformer clientcache.SharedIndexInformer,
	demandClient demandclient.ScalerV1alpha1Interface,
) *DemandCache {
	objectStore := store.NewStore()
	queue := store.NewShardedUniqueQueue(5)
	cache := newCache(queue, objectStore, demandInformer)
	asyncClient := &asyncClient{
		client:             demandClient.RESTClient(),
		resourceName:       demandapi.DemandCustomResourceDefinition().Spec.Names.Plural,
		emptyObjectCreator: func() object { return &demandapi.Demand{} },
		queue:              queue,
		objectStore:        objectStore,
	}
	return &DemandCache{
		cache:       cache,
		asyncClient: asyncClient,
	}
}

// Run starts the async clients of this cache
func (dc *DemandCache) Run(ctx context.Context) {
	dc.asyncClient.Run(ctx)
}

// Create enqueues a creation request and puts the object into the store
func (dc *DemandCache) Create(rr *demandapi.Demand) bool {
	return dc.cache.Create(rr)
}

// Delete enqueues a deletion request and removes the object from store
func (dc *DemandCache) Delete(rr *demandapi.Demand) {
	dc.cache.Delete(rr)
}

// Get returns the object from the store if it exists
func (dc *DemandCache) Get(namespace, name string) (*demandapi.Demand, bool) {
	obj, ok := dc.cache.Get(namespace, name)
	return obj.(*demandapi.Demand), ok
}
