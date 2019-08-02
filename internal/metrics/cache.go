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
	scalerlisters "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/listers/scaler/v1alpha1"
	sparkschedulerlisters "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/listers/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	"k8s.io/apimachinery/pkg/labels"
	"time"
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
	demandLister              scalerlisters.DemandLister
	resourceReservationCache  *cache.ResourceReservationCache
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
			rrs, err := c.resourceReservationLister.List(labels.Everything())
			if err != nil {
				svc1log.FromContext(ctx).Error("failed to list pods", svc1log.Stacktrace(err))
				break
			}
			rrsCached := c.resourceReservationCache.List()
			inflightCount := c.resourceReservationCache.InflightRequestCount()
			metrics.FromContext(ctx).Gauge(cachedObjectCount, rrTag, listerTag).Update(int64(len(rrs)))
			metrics.FromContext(ctx).Gauge(cachedObjectCount, rrTag, cacheTag).Update(int64(len(rrsCached)))
			metrics.FromContext(ctx).Gauge(inflightRequestCount, rrTag, cacheTag).Update(int64(inflightCount))
		}
	}
}
