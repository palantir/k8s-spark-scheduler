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
	"sync"
	"time"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha1"
	"github.com/palantir/k8s-spark-scheduler/internal/common/utils"
	"github.com/palantir/k8s-spark-scheduler/internal/crd"
	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientcache "k8s.io/client-go/tools/cache"
)

const (
	metricCacheCleanupInterval = 6 * time.Hour
)

var (
	beforeDemandCreation = tagInfo{
		tag:              metrics.MustNewTag(schedulingWasteTypeTagName, "before-demand-creation"),
		slowLogThreshold: 1 * time.Minute,
	}
	afterDemandFulfilled = tagInfo{
		tag:              metrics.MustNewTag(schedulingWasteTypeTagName, "after-demand-fulfilled"),
		slowLogThreshold: 1 * time.Minute,
	}
	totalTimeNoDemand = tagInfo{
		tag:              metrics.MustNewTag(schedulingWasteTypeTagName, "total-time-no-demand"),
		slowLogThreshold: 10 * time.Minute,
	}
)

type wasteMetricsReporter struct {
	ctx  context.Context
	info demandsByPod
	lock sync.Mutex
}

// StartSchedulingOverheadMetrics will start tracking demand creation an fulfillment times
// and report scheduling wasted time per pod
func StartSchedulingOverheadMetrics(
	ctx context.Context,
	podInformer coreinformers.PodInformer,
	demandInformer *crd.LazyDemandInformer,
) {
	reporter := &wasteMetricsReporter{
		ctx:  ctx,
		info: make(demandsByPod),
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
			informer.Informer().AddEventHandler(
				clientcache.FilteringResourceEventHandler{
					FilterFunc: utils.IsSparkSchedulerDemand,
					Handler: clientcache.ResourceEventHandlerFuncs{
						AddFunc: reporter.onDemandCreated,
						UpdateFunc: utils.OnDemandFulfilled(ctx, reporter.onDemandFulfilled),
					},
				},
			)
		}
	}()

	go func() {
		t := time.NewTicker(metricCacheCleanupInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				reporter.cleanupMetricCache()
			}
		}
	}()
}

type demandInfo struct {
	demandFulfilledTime time.Time
	demandCreationTime  time.Time
}

type podKey struct {
	Namespace string
	Name      string
}

type demandsByPod map[podKey]demandInfo

func (r *wasteMetricsReporter) onPodScheduled(pod *v1.Pod) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if info, ok := r.info[podKey{pod.Namespace, pod.Name}]; ok {
		r.markAndSlowLog(pod, beforeDemandCreation, info.demandCreationTime.Sub(pod.CreationTimestamp.Time))
		if !info.demandFulfilledTime.IsZero() {
			r.markAndSlowLog(pod, afterDemandFulfilled, time.Now().Sub(info.demandFulfilledTime))
		}
	} else {
		r.markAndSlowLog(pod, totalTimeNoDemand, time.Now().Sub(pod.CreationTimestamp.Time))
	}
}

func (r *wasteMetricsReporter) markAndSlowLog(pod *v1.Pod, tag tagInfo, duration time.Duration) {
	if duration > tag.slowLogThreshold {
		svc1log.FromContext(r.ctx).Info("pod wait time is above threshold",
			svc1log.SafeParam("podNamespace", pod.Namespace),
			svc1log.SafeParam("podName", pod.Name),
			svc1log.SafeParam("waitType", tag.tag.Value()),
			svc1log.SafeParam("duration", duration))
	}
	metrics.FromContext(r.ctx).Histogram(schedulingWaste, tag.tag).Update(duration.Nanoseconds())
}

func (r *wasteMetricsReporter) onDemandFulfilled(demand *v1alpha1.Demand) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.info[podKey{demand.Namespace, utils.PodName(demand)}] = demandInfo{
		demandFulfilledTime: time.Now(),
		demandCreationTime:  demand.CreationTimestamp.Time,
	}
}

func (r *wasteMetricsReporter) onDemandCreated(obj interface{}) {
	r.lock.Lock()
	defer r.lock.Unlock()
	demand, ok := obj.(*v1alpha1.Demand)
	if !ok {
		svc1log.FromContext(r.ctx).Error("failed to parse obj as demand")
		return
	}
	r.info[podKey{demand.Namespace, utils.PodName(demand)}] = demandInfo{
		demandCreationTime:  demand.CreationTimestamp.Time,
	}
}

func (r *wasteMetricsReporter) cleanupMetricCache() {
	r.lock.Lock()
	defer r.lock.Unlock()
	for key, info := range r.info {
		if info.demandFulfilledTime.Add(metricCacheCleanupInterval).Before(time.Now()) {
			svc1log.FromContext(r.ctx).Info(
				"deleting demand from scheduling waste reporter, pod was not scheduled for 6 hours",
				svc1log.SafeParam("podNamespace", key.Namespace),
				svc1log.SafeParam("podNamespace", key.Name),
			)
			delete(r.info, key)
		}
	}

}

type tagInfo struct {
	tag              metrics.Tag
	slowLogThreshold time.Duration
}
