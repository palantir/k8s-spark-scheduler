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
	"github.com/palantir/k8s-spark-scheduler/internal/reservations"
	"time"

	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/pkg/metrics"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
)

// SoftReservationMetrics reports metrics on the SoftReservationStore passed
type SoftReservationMetrics struct {
	softReservationStore *cache.SoftReservationStore
	podLister            corelisters.PodLister
	resourceReservations reservations.Store
	logger               svc1log.Logger
}

// NewSoftReservationMetrics creates a SoftReservationMetrics
func NewSoftReservationMetrics(ctx context.Context, store *cache.SoftReservationStore, podLister corelisters.PodLister, resourceReservations reservations.Store) *SoftReservationMetrics {
	return &SoftReservationMetrics{
		softReservationStore: store,
		podLister:            podLister,
		resourceReservations: resourceReservations,
		logger:               svc1log.FromContext(ctx),
	}
}

// StartReporting starts periodic reporting for SoftReservationStore metrics
func (s *SoftReservationMetrics) StartReporting(ctx context.Context) {
	_ = wapp.RunWithFatalLogging(ctx, s.doStart)
}

func (s *SoftReservationMetrics) doStart(ctx context.Context) error {
	t := time.NewTicker(tickInterval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			s.emitSoftReservationMetrics(ctx)
		}
	}
}

func (s *SoftReservationMetrics) emitSoftReservationMetrics(ctx context.Context) {
	softReservationsCount := s.softReservationStore.GetApplicationCount()
	extraExecutorCount := s.softReservationStore.GetActiveExtraExecutorCount()
	execWithNoReservationCount, err := s.getAllocatedExecutorsWithNoReservationCount(ctx)
	if err != nil {
		s.logger.Error("failed to emit executors with no reservation count metric", svc1log.Stacktrace(err))
	} else {
		metrics.FromContext(ctx).Gauge(executorsWithNoReservationCount).Update(int64(execWithNoReservationCount))
	}

	metrics.FromContext(ctx).Gauge(softReservationCount).Update(int64(softReservationsCount))
	metrics.FromContext(ctx).Gauge(softReservationExecutorCount).Update(int64(extraExecutorCount))
}

func (s *SoftReservationMetrics) getAllocatedExecutorsWithNoReservationCount(ctx context.Context) (int, error) {
	podsWithNoReservationCount := 0
	rrs, err := s.resourceReservations.List(ctx)
	if err != nil {
		return 0, err
	}
	podsWithRRs := make(map[string]bool, len(rrs))
	for _, rr := range rrs {
		for _, podName := range rr.Status.Pods {
			podsWithRRs[podName] = true
		}
	}
	pods, err := s.podLister.List(labels.Everything())
	if err != nil {
		return 0, werror.Wrap(err, "failed to list pods")
	}
	for _, pod := range pods {
		if podsWithRRs[pod.Name] {
			continue
		}
		role, ok := pod.Labels[sparkRoleLabel]
		if !ok || role != executor || pod.Spec.SchedulerName != sparkSchedulerName || pod.Spec.NodeName == "" || pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
			continue
		}
		podsWithNoReservationCount++
	}
	return podsWithNoReservationCount, nil
}
