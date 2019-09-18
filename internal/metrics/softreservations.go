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

	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
)

// SoftReservationMetrics reports metrics on the SoftReservationStore passed
type SoftReservationMetrics struct {
	softReservationStore *cache.SoftReservationStore
}

// NewSoftReservationMetrics creates a SoftReservationMetrics
func NewSoftReservationMetrics(store *cache.SoftReservationStore) *SoftReservationMetrics {
	return &SoftReservationMetrics{
		softReservationStore: store,
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
	softReservations := s.softReservationStore.GetAllSoftReservations()
	var executorCount = 0
	for _, sr := range softReservations {
		executorCount += len(sr.Reservations)
	}

	metrics.FromContext(ctx).Gauge(softReservationCount).Update(int64(len(softReservations)))
	metrics.FromContext(ctx).Gauge(softReservationExecutorCount).Update(int64(executorCount))
}
