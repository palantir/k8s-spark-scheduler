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

package metrics

import (
	"context"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha1"
	"github.com/palantir/k8s-spark-scheduler/internal/common/utils"
	"github.com/palantir/k8s-spark-scheduler/internal/crd"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientcache "k8s.io/client-go/tools/cache"
	"sync"
	"time"
)

const (
	cleanupInterval = 6 * time.Hour
)

type WasteMetricsReporter struct {
	ctx context.Context
	schedulingInfo SchedulingInfo
	lock sync.Mutex
}

func StartSchedulingOverheadMetrics(
	ctx context.Context,
	podInformer coreinformers.PodInformer,
	demandInformer crd.LazyDemandInformer,
	) {
	reporter := &WasteMetricsReporter{
		ctx: ctx,
		schedulingInfo: make(SchedulingInfo),
	}

	podInformer.Informer().AddEventHandler(
		clientcache.FilteringResourceEventHandler{
			FilterFunc: utils.IsSparkSchedulerPod,
			Handler: clientcache.ResourceEventHandlerFuncs{
				UpdateFunc: utils.OnPodScheduled(ctx, reporter.onPodScheduled),
			},
		},
	)
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-demandInformer.Ready():
			informer, _ := demandInformer.Informer()
			informer.Informer().AddEventHandler(clientcache.ResourceEventHandlerFuncs{
				UpdateFunc: utils.OnDemandFulfilled(ctx, reporter.onDemandFulfilled),
			})
		}
	}()

	go func() {
		t := time.NewTicker(cleanupInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <- t.C:
				reporter.cleanup()
			}
		}
	}()
}

type DemandInfo struct {
	demandFulfilledTime time.Time
	demandCreationTime  time.Time
}

type PodKey struct {
	Namespace string
	Name      string
}

type SchedulingInfo map[PodKey]DemandInfo


func (r *WasteMetricsReporter) onPodScheduled(pod *v1.Pod) {
	r.lock.Lock()
	defer r.lock.Unlock()
	// TODO: report
}

func (r *WasteMetricsReporter) onDemandFulfilled(demand *v1alpha1.Demand) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.schedulingInfo[PodKey{demand.Namespace, utils.PodName(demand)}] = DemandInfo{
		demandFulfilledTime: time.Now(),
		demandCreationTime:  demand.CreationTimestamp.Time,
	}
}

func (r *WasteMetricsReporter) cleanup() {
	r.lock.Lock()
	defer r.lock.Unlock()
	for key, demandInfo := range r.schedulingInfo {
		if demandInfo.demandFulfilledTime.Add(cleanupInterval).Before(time.Now()) {
			svc1log.FromContext(r.ctx).Info(
				"deleting demand from scheduling waste reporter, pod was not scheduled for 6 hours",
				svc1log.SafeParam("podNamespace", key.Namespace),
				svc1log.SafeParam("podNamespace", key.Name),
			)
			delete(r.schedulingInfo, key)
		}
	}

}
