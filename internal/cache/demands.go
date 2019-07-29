package cache

import (
	"context"
	demandapi "github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha1"
	demandclient "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/scaler/v1alpha1"
	"github.com/palantir/k8s-spark-scheduler/internal/cache/store"
	clientcache "k8s.io/client-go/tools/cache"
)

type DemandCache struct {
	cache       *cache
	asyncClient *asyncClient
}

func NewDemandCache(
	demandInformer clientcache.SharedIndexInformer,
	demandClient demandclient.ScalerV1alpha1Interface,
) *DemandCache {
	objectStore := store.NewStore()
	queue := store.NewShardedUniqueQueue(5)
	cache := NewCache(queue, objectStore, demandInformer)
	asyncClient := NewAsyncClient(
		demandClient.RESTClient(),
		demandapi.DemandCustomResourceDefinition().Spec.Names.Plural,
		func() object { return &demandapi.Demand{} },
		queue,
	)
	return &DemandCache{
		cache:       cache,
		asyncClient: asyncClient,
	}
}

func (dc *DemandCache) Run(ctx context.Context) {
	dc.asyncClient.Run(ctx)
}

func (dc *DemandCache) Create(rr *demandapi.Demand) bool {
	return dc.cache.Create(rr)
}

func (dc *DemandCache) Update(rr *demandapi.Demand) bool {
	return dc.cache.Update(rr)
}

func (dc *DemandCache) Delete(rr *demandapi.Demand) {
	dc.cache.Delete(rr)
}

func (dc *DemandCache) Get(namespace, name string) (*demandapi.Demand, bool) {
	obj, ok := dc.cache.Get(namespace, name)
	return obj.(*demandapi.Demand), ok
}

func (dc *DemandCache) List() []*demandapi.Demand {
	objects := dc.cache.List()
	res := make([]*demandapi.Demand, 0, len(objects))
	for _, o := range objects {
		res = append(res, o.(*demandapi.Demand))
	}
	return res
}
