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

package crd

import (
	"context"
	"time"

	demandapi "github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha2"
	ssinformers "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/informers/externalversions"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/client/informers/externalversions/scaler/v1alpha2"
	"github.com/palantir/pkg/retry"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	clientcache "k8s.io/client-go/tools/cache"
)

const (
	informerSyncRetryCount          = 5
	informerSyncTimeout             = 2 * time.Second
	informerSyncRetryInitialBackoff = 500 * time.Millisecond
)

// LazyDemandInformer checks for Demand CRD existence and creates a
// demand informer if it exists.
type LazyDemandInformer struct {
	informerFactory     ssinformers.SharedInformerFactory
	apiExtensionsClient apiextensionsclientset.Interface
	ready               chan struct{}
	informer            v1alpha2.DemandInformer
	tickerTime          time.Duration
}

// NewLazyDemandInformer constructs a new LazyDemandInformer instance
func NewLazyDemandInformer(
	informerFactory ssinformers.SharedInformerFactory,
	apiExtensionsClient apiextensionsclientset.Interface,
	tickerTime time.Duration) *LazyDemandInformer {
	return &LazyDemandInformer{
		informerFactory:     informerFactory,
		apiExtensionsClient: apiExtensionsClient,
		tickerTime:          tickerTime,
		ready:               make(chan struct{}),
	}
}

// Informer returns the informer instance if it is initialized, returns false otherwise
func (ldi *LazyDemandInformer) Informer() (v1alpha2.DemandInformer, bool) {
	select {
	case <-ldi.Ready():
		return ldi.informer, true
	default:
		return nil, false
	}
}

// Ready returns a channel that will be closed when the informer is initialized
func (ldi *LazyDemandInformer) Ready() <-chan struct{} {
	return ldi.ready
}

// Run starts the goroutine to check for the existence of the demand CRD,
// and initialize the demand informer if CRD exists
func (ldi *LazyDemandInformer) Run(ctx context.Context) {
	if ldi.initializeInformer(ctx) {
		return
	}
	go func() {
		_ = wapp.RunWithFatalLogging(ctx, ldi.doStart)
	}()
}

func (ldi *LazyDemandInformer) doStart(ctx context.Context) error {
	t := time.NewTicker(ldi.tickerTime)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if ldi.initializeInformer(ctx) {
				return nil
			}
		}
	}
}

func (ldi *LazyDemandInformer) initializeInformer(ctx context.Context) bool {
	_, ready, err := CheckCRDExists(ctx, demandapi.DemandCustomResourceDefinitionName(), ldi.apiExtensionsClient)
	if err != nil {
		svc1log.FromContext(ctx).Info("failed to determine if demand CRD exists", svc1log.Stacktrace(err))
		return false
	}
	if !ready {
		return false
	}
	svc1log.FromContext(ctx).Info("demand CRD has been initialized. Demand resources can now be created")
	informer, err := ldi.createInformer(ctx)
	if err != nil {
		svc1log.FromContext(ctx).Error("failed initializing demand informer", svc1log.Stacktrace(err))
		return false
	}

	ldi.informer = informer
	close(ldi.ready)
	return ready
}

func (ldi *LazyDemandInformer) createInformer(ctx context.Context) (v1alpha2.DemandInformer, error) {
	informerInterface := ldi.informerFactory.Scaler().V1alpha2().Demands()
	informer := informerInterface.Informer()
	ldi.informerFactory.Start(ctx.Done())

	err := retry.Do(ctx, func() error {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, informerSyncTimeout)
		defer cancel()
		if ok := clientcache.WaitForCacheSync(ctxWithTimeout.Done(), informer.HasSynced); !ok {
			return werror.ErrorWithContextParams(ctx, "timeout syncing informer", werror.SafeParam("timeoutSeconds", informerSyncTimeout.Seconds()))
		}
		return nil
	}, retry.WithMaxAttempts(informerSyncRetryCount), retry.WithInitialBackoff(informerSyncRetryInitialBackoff))

	if err != nil {
		return nil, err
	}
	return informerInterface, nil
}
