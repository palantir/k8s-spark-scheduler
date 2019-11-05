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
	"sync"
	"time"

	demandapi "github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha1"
	demandclient "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/scaler/v1alpha1"
	ssinformers "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/informers/externalversions"
	"github.com/palantir/k8s-spark-scheduler/internal/crd"
	"github.com/palantir/pkg/retry"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	"go.uber.org/atomic"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	clientcache "k8s.io/client-go/tools/cache"
)

// SafeDemandCache wraps a demand cache by checking if the demand
// CRD exists before each operation
type SafeDemandCache struct {
	*DemandCache
	demandCRDInitialized atomic.Bool
	informerFactory      ssinformers.SharedInformerFactory
	apiExtensionsClient  apiextensionsclientset.Interface
	cacheInitialization  sync.Mutex
	demandKubeClient     demandclient.ScalerV1alpha1Interface
}

// NewSafeDemandCache returns a demand cache which fallbacks
// to no-op if demand CRD doesn't exist
func NewSafeDemandCache(
	informerFactory ssinformers.SharedInformerFactory,
	apiExtensionsClient apiextensionsclientset.Interface,
	demandKubeClient demandclient.ScalerV1alpha1Interface,
) *SafeDemandCache {
	return &SafeDemandCache{
		informerFactory:     informerFactory,
		apiExtensionsClient: apiExtensionsClient,
		demandKubeClient:    demandKubeClient,
	}
}

// Run starts the goroutine to check for the existence of the demand CRD
func (sdc *SafeDemandCache) Run(ctx context.Context) {
	if sdc.checkDemandCRDExists(ctx) {
		return
	}
	go func() {
		_ = wapp.RunWithFatalLogging(ctx, sdc.doStart)
	}()
}

func (sdc *SafeDemandCache) doStart(ctx context.Context) error {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if sdc.checkDemandCRDExists(ctx) {
				return nil
			}
		}
	}
}

func (sdc *SafeDemandCache) checkDemandCRDExists(ctx context.Context) bool {
	_, ready, err := crd.CheckCRDExists(demandapi.DemandCustomResourceDefinitionName(), sdc.apiExtensionsClient)
	if err != nil {
		svc1log.FromContext(ctx).Info("failed to determine if demand CRD exists", svc1log.Stacktrace(err))
		return false
	}
	if ready {
		svc1log.FromContext(ctx).Info("demand CRD has been initialized. Demand resources can now be created")
		err = sdc.initializeCache(ctx)
		if err != nil {
			svc1log.FromContext(ctx).Error("failed initializing demand cache", svc1log.Stacktrace(err))
			return false
		}
	}
	return ready
}

func (sdc *SafeDemandCache) initializeCache(ctx context.Context) error {
	sdc.cacheInitialization.Lock()
	defer sdc.cacheInitialization.Unlock()
	if sdc.demandCRDInitialized.Load() {
		return nil
	}
	informerInterface := sdc.informerFactory.Scaler().V1alpha1().Demands()
	informer := informerInterface.Informer()
	sdc.informerFactory.Start(ctx.Done())

	ctxWithTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if ok := clientcache.WaitForCacheSync(ctxWithTimeout.Done(), informer.HasSynced); !ok {
		return werror.Error("timeout syncing informer", werror.SafeParam("timeoutSeconds", 2))
	}
	demandCache, err := NewDemandCache(ctx, informerInterface, sdc.demandKubeClient)
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
