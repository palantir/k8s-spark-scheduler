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
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	sparkschedulerclient "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/sparkscheduler/v1beta1"
	rrinformers "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/informers/externalversions/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/k8s-spark-scheduler/internal/cache/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	// resourceReservationClients denotes the number of
	// kube clients that will issue write requests for
	// resource reservations in parallel.
	resourceReservationClients = 5
)

// ResourceReservationCache is a cache for resource reservations.
// It assumes it is the only client that issues write requests for
// resource reservations. Any external update and creation will be
// ignored, but deletions will be reflected in the cache.
type ResourceReservationCache struct {
	client      sparkschedulerclient.SparkschedulerV1beta1Interface
	cache       *cache
	asyncClient *asyncClient
}

// NewResourceReservationCache creates a new cache.
func NewResourceReservationCache(
	ctx context.Context,
	resourceReservationInformer rrinformers.ResourceReservationInformer,
	resourceReservationKubeClient sparkschedulerclient.SparkschedulerV1beta1Interface,
	asyncClientConfig config.AsyncClientConfig,
) (*ResourceReservationCache, error) {
	rrs, err := resourceReservationInformer.Lister().List(labels.Everything())
	if err != nil {
		return nil, err
	}
	objectStore := store.NewStore(ctx)
	for _, rr := range rrs {
		objectStore.Put(rr)
	}
	queue := store.NewShardedUniqueQueue(resourceReservationClients)
	cache := newCache(ctx, queue, objectStore, resourceReservationInformer.Informer())
	asyncClient := &asyncClient{
		client:      &resourceReservationClient{resourceReservationKubeClient},
		queue:       queue,
		objectStore: objectStore,
		config:      asyncClientConfig,
		metrics:     &AsyncClientMetrics{ObjectTypeTag: "resourcereservations"},
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
func (rrc *ResourceReservationCache) Create(rr *v1beta1.ResourceReservation) error {
	return rrc.cache.Create(rr)
}

// Update enqueues an update request and updates the object in store
func (rrc *ResourceReservationCache) Update(rr *v1beta1.ResourceReservation) error {
	return rrc.cache.Update(rr)
}

// Delete enqueues a deletion request and removes the object from store
func (rrc *ResourceReservationCache) Delete(namespace, name string) {
	rrc.cache.Delete(namespace, name)
}

// Get returns the object from the store if it exists
func (rrc *ResourceReservationCache) Get(namespace, name string) (*v1beta1.ResourceReservation, bool) {
	obj, ok := rrc.cache.Get(namespace, name)
	if !ok {
		return nil, false
	}
	return obj.(*v1beta1.ResourceReservation), true
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

// InflightQueueLengths returns the number of items per request queue
func (rrc *ResourceReservationCache) InflightQueueLengths() []int {
	return rrc.cache.queue.QueueLengths()
}

type resourceReservationClient struct {
	sparkschedulerclient.SparkschedulerV1beta1Interface
}

func (client *resourceReservationClient) Create(obj metav1.Object) (metav1.Object, error) {
	return client.ResourceReservations(obj.GetNamespace()).Create(obj.(*v1beta1.ResourceReservation))
}

func (client *resourceReservationClient) Update(obj metav1.Object) (metav1.Object, error) {
	return client.ResourceReservations(obj.GetNamespace()).Update(obj.(*v1beta1.ResourceReservation))
}

func (client *resourceReservationClient) Delete(namespace, name string) error {
	return client.ResourceReservations(namespace).Delete(name, nil) // TODO options
}

func (client *resourceReservationClient) Get(namespace, name string) (metav1.Object, error) {
	return client.ResourceReservations(namespace).Get(name, metav1.GetOptions{})
}
