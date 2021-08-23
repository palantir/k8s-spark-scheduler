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

	demandapi "github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha2"
	demandclient "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/scaler/v1alpha2"
	demandinformers "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/informers/externalversions/scaler/v1alpha2"
	"github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/k8s-spark-scheduler/internal/cache/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	// demandClients denotes the number of
	// kube clients that will issue write requests for
	// demands in parallel.
	demandClients = 5
)

// DemandCache is a cache for demands. It assumes it is the only
// client that creates demands. Externally created demands will not be
// included in the cache. Deletions from all clients are valid and are
// reflected in the cache.
type DemandCache struct {
	cache       *cache
	asyncClient *asyncClient
}

// NewDemandCache creates a new cache
func NewDemandCache(
	ctx context.Context,
	demandInformer demandinformers.DemandInformer,
	demandKubeClient demandclient.ScalerV1alpha2Interface,
	asyncClientConfig config.AsyncClientConfig,
) (*DemandCache, error) {
	ds, err := demandInformer.Lister().List(labels.Everything())
	if err != nil {
		return nil, err
	}
	objectStore := store.NewStore(ctx)
	for _, d := range ds {
		objectStore.Put(d)
	}
	queue := store.NewShardedUniqueQueue(demandClients)
	cache := newCache(ctx, queue, objectStore, demandInformer.Informer())
	asyncClient := &asyncClient{
		client:      &demandClient{demandKubeClient},
		queue:       queue,
		objectStore: objectStore,
		config:      asyncClientConfig,
		metrics:     &AsyncClientMetrics{ObjectTypeTag: "demands"},
	}
	return &DemandCache{
		cache:       cache,
		asyncClient: asyncClient,
	}, nil
}

// Run starts the async clients of this cache
func (dc *DemandCache) Run(ctx context.Context) {
	dc.asyncClient.Run(ctx)
}

// Create enqueues a creation request and puts the object into the store
func (dc *DemandCache) Create(rr *demandapi.Demand) error {
	return dc.cache.Create(rr)
}

// Delete enqueues a deletion request and removes the object from store
func (dc *DemandCache) Delete(namespace, name string) {
	dc.cache.Delete(namespace, name)
}

// Get returns the object from the store if it exists
func (dc *DemandCache) Get(namespace, name string) (*demandapi.Demand, bool) {
	obj, ok := dc.cache.Get(namespace, name)
	if !ok {
		return nil, false
	}
	return obj.(*demandapi.Demand), true
}

type demandClient struct {
	demandclient.ScalerV1alpha2Interface
}

func (client *demandClient) Create(ctx context.Context, obj metav1.Object) (metav1.Object, error) {
	return client.Demands(obj.GetNamespace()).Create(ctx, obj.(*demandapi.Demand), metav1.CreateOptions{})
}

func (client *demandClient) Update(ctx context.Context, obj metav1.Object) (metav1.Object, error) {
	return client.Demands(obj.GetNamespace()).Update(ctx, obj.(*demandapi.Demand), metav1.UpdateOptions{})
}

func (client *demandClient) Delete(ctx context.Context, namespace, name string) error {
	return client.Demands(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

func (client *demandClient) Get(ctx context.Context, namespace, name string) (metav1.Object, error) {
	return client.Demands(namespace).Get(ctx, name, metav1.GetOptions{})
}
