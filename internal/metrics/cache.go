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
	"math"
	"time"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	sparkschedulerlisters "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/listers/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

const (
	informerDelayBuffer = 5
)

var (
	listerTag = metrics.MustNewTag("source", "lister")
	cacheTag  = metrics.MustNewTag("source", "cache")
	rrTag     = metrics.MustNewTag("cachedobject", "resourcereservation")
	demandTag = metrics.MustNewTag("cachedobject", "demand")
)

// CacheMetrics reports metrics for resource reservation and demand caches
type CacheMetrics struct {
	resourceReservationLister sparkschedulerlisters.ResourceReservationLister
	resourceReservations      *cache.ResourceReservationCache
	demands                   *cache.SafeDemandCache
}

// NewCacheMetrics creates a new CacheMetrics object
func NewCacheMetrics(
	resourceReservationLister sparkschedulerlisters.ResourceReservationLister,
	resourceReservations *cache.ResourceReservationCache,
	demands *cache.SafeDemandCache,
) *CacheMetrics {
	return &CacheMetrics{
		resourceReservationLister: resourceReservationLister,
		resourceReservations:      resourceReservations,
		demands:                   demands,
	}
}

// StartReporting starts periodic reporting for cache metrics
func (c *CacheMetrics) StartReporting(ctx context.Context) {
	_ = wapp.RunWithFatalLogging(ctx, c.doStart)
}

func (c *CacheMetrics) doStart(ctx context.Context) error {
	t := time.NewTicker(tickInterval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			c.emitResourceReservationMetrics(ctx)
			if c.demands.CRDExists() {
				c.emitDemandMetrics(ctx)
			}
		}
	}
}

func (c *CacheMetrics) emitResourceReservationMetrics(ctx context.Context) {
	rrs, err := c.resourceReservationLister.List(labels.Everything())
	if err != nil {
		svc1log.FromContext(ctx).Error("failed to list resource reservations", svc1log.Stacktrace(err))
		return
	}
	rrsCached := c.resourceReservations.List()
	metrics.FromContext(ctx).Gauge(cachedObjectCount, rrTag, listerTag).Update(int64(len(rrs)))
	metrics.FromContext(ctx).Gauge(cachedObjectCount, rrTag, cacheTag).Update(int64(len(rrsCached)))
	totalQueueLength := 0
	for idx, queueLength := range c.resourceReservations.InflightQueueLengths() {
		metrics.FromContext(ctx).Gauge(inflightRequestCount, rrTag, QueueIndexTag(ctx, idx)).Update(int64(queueLength))
		totalQueueLength += queueLength
	}
	if int(math.Abs(float64(len(rrs)-len(rrsCached)))) > totalQueueLength+informerDelayBuffer {
		svc1log.FromContext(ctx).Warn("found unexplained cache size difference",
			svc1log.SafeParam("rrsCached", len(rrsCached)),
			svc1log.SafeParam("rrs", len(rrs)))
		reportDifference(ctx, rrsCached, rrs)
	}
}

func (c *CacheMetrics) emitDemandMetrics(ctx context.Context) {
	demandsCached := c.demands.CacheSize()
	metrics.FromContext(ctx).Gauge(cachedObjectCount, demandTag, cacheTag).Update(int64(demandsCached))
	for idx, queueLength := range c.demands.InflightQueueLengths() {
		metrics.FromContext(ctx).Gauge(inflightRequestCount, demandTag, QueueIndexTag(ctx, idx)).Update(int64(queueLength))
	}
}

func reportDifference(ctx context.Context, cached, actual []*v1beta1.ResourceReservation) {
	cachedMap := toMap(cached)
	for _, rr := range actual {
		if _, ok := cachedMap[rr.UID]; !ok {
			svc1log.FromContext(ctx).Warn("resource reservation only exists in lister", rrParams(rr))
		}
	}
	actualMap := toMap(actual)
	for _, rr := range cached {
		if _, ok := actualMap[rr.UID]; !ok {
			svc1log.FromContext(ctx).Warn("resource reservation only exists in cache", rrParams(rr))
		}
	}
}

func toMap(rrs []*v1beta1.ResourceReservation) map[types.UID]*v1beta1.ResourceReservation {
	res := make(map[types.UID]*v1beta1.ResourceReservation, len(rrs))
	for _, rr := range rrs {
		res[rr.UID] = rr
	}
	return res
}

func rrParams(rr *v1beta1.ResourceReservation) svc1log.Param {
	return svc1log.SafeParams(map[string]interface{}{
		"resourceReservationName":      rr.Name,
		"resourceReservationNamespace": rr.Namespace,
	})
}
