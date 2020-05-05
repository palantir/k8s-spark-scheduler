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
	"github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/k8s-spark-scheduler/internal/crd"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	"go.uber.org/atomic"
)

// SafeDemandCache wraps a demand cache by checking if the demand
// CRD exists before each operation
type SafeDemandCache struct {
	*DemandCache
	demandCRDInitialized atomic.Bool
	lazyDemandInformer   *crd.LazyDemandInformer
	demandKubeClient     demandclient.ScalerV1alpha1Interface
	asyncClientConfig    config.AsyncClientConfig
}

// NewSafeDemandCache returns a demand cache which fallbacks
// to no-op if demand CRD doesn't exist
func NewSafeDemandCache(
	lazyDemandInformer *crd.LazyDemandInformer,
	demandKubeClient demandclient.ScalerV1alpha1Interface,
	asyncClientConfig config.AsyncClientConfig,
) *SafeDemandCache {
	return &SafeDemandCache{
		lazyDemandInformer: lazyDemandInformer,
		demandKubeClient:   demandKubeClient,
		asyncClientConfig:  asyncClientConfig,
	}
}

// Run starts the goroutine to check for the existence of the demand CRD
func (sdc *SafeDemandCache) Run(ctx context.Context) {
	err := sdc.initializeCache(ctx)
	if err == nil {
		return
	}
	go func() {
		err := wapp.RunWithFatalLogging(ctx, sdc.wait)
		if err != nil {
			panic(err)
		}
	}()
}

func (sdc *SafeDemandCache) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-sdc.lazyDemandInformer.Ready():
		return sdc.initializeCache(ctx)
	}
}

func (sdc *SafeDemandCache) initializeCache(ctx context.Context) error {
	if sdc.demandCRDInitialized.Load() {
		return nil
	}
	informer, ok := sdc.lazyDemandInformer.Informer()
	if !ok {
		return werror.ErrorWithContextParams(ctx, "demand informer not initialized yet")
	}
	demandCache, err := NewDemandCache(ctx, informer, sdc.demandKubeClient, sdc.asyncClientConfig)
	if err != nil {
		return err
	}
	demandCache.Run(ctx)
	sdc.DemandCache = demandCache
	sdc.demandCRDInitialized.Store(true)
	return nil
}

// CRDExists checks if the demand crd exists
func (sdc *SafeDemandCache) CRDExists() bool {
	return sdc.demandCRDInitialized.Load()
}

// Create enqueues a creation request and puts the object into the store
func (sdc *SafeDemandCache) Create(rr *demandapi.Demand) error {
	if !sdc.demandCRDInitialized.Load() {
		return werror.Error("Can not create demand because demand CRD does not exist")
	}
	return sdc.DemandCache.Create(rr)
}

// Delete enqueues a deletion request and removes the object from store
func (sdc *SafeDemandCache) Delete(namespace, name string) {
	if !sdc.demandCRDInitialized.Load() {
		return
	}
	sdc.DemandCache.Delete(namespace, name)
}

// Get returns the object from the store if it exists
func (sdc *SafeDemandCache) Get(namespace, name string) (*demandapi.Demand, bool) {
	if !sdc.demandCRDInitialized.Load() {
		return nil, false
	}
	return sdc.DemandCache.Get(namespace, name)
}

// CacheSize returns the number of elements in the cache
func (sdc *SafeDemandCache) CacheSize() int {
	return len(sdc.cache.List())
}

// InflightQueueLengths returns the number of items per request queue
func (sdc *SafeDemandCache) InflightQueueLengths() []int {
	return sdc.cache.queue.QueueLengths()
}
