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

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
)

// ResourceReservationMetrics reports metrics on the ResourceReservationManager passed
type ResourceReservationMetrics struct {
	totalUnboundResourcesReservationsFn func() *resources.Resources
}

// NewResourceReservationMetrics creates a ResourceReservationMetrics
// func NewResourceReservationMetrics(resourceReservationManager *extender.ResourceReservationManager) *ResourceReservationMetrics {
func NewResourceReservationMetrics(totalUnboundResourcesReservationsFn func() *resources.Resources) *ResourceReservationMetrics {
	return &ResourceReservationMetrics{
		totalUnboundResourcesReservationsFn: totalUnboundResourcesReservationsFn,
	}
}

// StartReporting starts periodic reporting for ResourceReservationManager metrics
func (s *ResourceReservationMetrics) StartReporting(ctx context.Context) {
	_ = wapp.RunWithFatalLogging(ctx, s.doStart)
}

func (s *ResourceReservationMetrics) doStart(ctx context.Context) error {
	t := time.NewTicker(tickInterval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			s.emitUnboundResourceReservationMetrics(ctx)
		}
	}
}

func (s *ResourceReservationMetrics) emitUnboundResourceReservationMetrics(ctx context.Context) {
	unboundReservedResources := s.totalUnboundResourcesReservationsFn()
	metrics.FromContext(ctx).GaugeFloat64(unboundCPUReservations).Update(unboundReservedResources.CPU.AsApproximateFloat64())
	metrics.FromContext(ctx).GaugeFloat64(unboundMemoryReservations).Update(unboundReservedResources.Memory.AsApproximateFloat64())
	metrics.FromContext(ctx).GaugeFloat64(unboundNvidiaGPUReservations).Update(unboundReservedResources.NvidiaGPU.AsApproximateFloat64())
}
