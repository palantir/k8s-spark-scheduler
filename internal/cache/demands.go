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
	demandapi "github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha1"
	demandclient "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/scaler/v1alpha1"
	demandinformers "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/informers/externalversions/scaler/v1alpha1"
	"github.com/palantir/k8s-spark-scheduler/internal/cache/store"
	"k8s.io/apimachinery/pkg/labels"
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
	demandInformer demandinformers.DemandInformer,
	demandClient demandclient.ScalerV1alpha1Interface,
) (*DemandCache, error) {
	ds, err := demandInformer.Lister().List(labels.Everything())
	if err != nil {
		return nil, err
	}
	objectStore := store.NewStore()
	for _, d := range ds {
		objectStore.Put(d)
	}
	queue := store.NewShardedUniqueQueue(5)
	cache := newCache(queue, objectStore, demandInformer.Informer())
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
	}, nil
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
