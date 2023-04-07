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
	"time"

	sparkschedulerlisters "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/listers/sparkscheduler/v1beta2"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
)

const (
	informerDelayBuffer = 5
)

var (
	cacheTag  = metrics.MustNewTag("source", "cache")
	demandTag = metrics.MustNewTag("cachedobject", "demand")
)

// CacheMetrics reports metrics for resource reservation and demand caches
type CacheMetrics struct {
	resourceReservationLister sparkschedulerlisters.ResourceReservationLister
	demands                   *cache.SafeDemandCache
}

// NewCacheMetrics creates a new CacheMetrics object
func NewCacheMetrics(
	resourceReservationLister sparkschedulerlisters.ResourceReservationLister,
	demands *cache.SafeDemandCache,
) *CacheMetrics {
	return &CacheMetrics{
		resourceReservationLister: resourceReservationLister,
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
			if c.demands.CRDExists() {
				c.emitDemandMetrics(ctx)
			}
		}
	}
}

func (c *CacheMetrics) emitDemandMetrics(ctx context.Context) {
	demandsCached := c.demands.CacheSize()
	metrics.FromContext(ctx).Gauge(cachedObjectCount, demandTag, cacheTag).Update(int64(demandsCached))
	for idx, queueLength := range c.demands.InflightQueueLengths() {
		metrics.FromContext(ctx).Gauge(inflightRequestCount, demandTag, QueueIndexTag(ctx, idx)).Update(int64(queueLength))
	}
}
