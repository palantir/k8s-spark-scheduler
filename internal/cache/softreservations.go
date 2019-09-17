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

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientcache "k8s.io/client-go/tools/cache"
)

// TODO(rkaram): Move to common place to avoid duplication without causing circular dependency
const (
	// SparkRoleLabel represents the label key for the spark-role of a pod
	SparkRoleLabel = "spark-role"
	// SparkAppIDLabel represents the label key for the spark application ID on a pod
	SparkAppIDLabel = "spark-app-id" // TODO(onursatici): change this to a spark specific label when spark has one
	// Driver represents the label key for a pod that identifies the pod as a spark driver
	Driver = "driver"
	// Executor represents the label key for a pod that identifies the pod as a spark executor
	Executor = "executor"
)

// SoftReservationStore is an in-memory store that keeps track of soft reservations granted to extra executors for applications that support dynamic allocation
type SoftReservationStore struct {
	store     map[string]*SoftReservation // SparkAppID -> SoftReservation
	storeLock sync.RWMutex
}

// SoftReservation is an in-memory reservation for a particular spark application that keeps track of extra executors allocate over the
// min reservation count
// TODO(rkaram): check if we want to use the same reservation object we already have
type SoftReservation struct {
	Reservations map[string]v1beta1.Reservation // Executor pod name -> Reservation (only valid ones here)
	Status       map[string]bool                // Executor pod name -> Reservation valid or not
}

// NewSoftReservationStore builds and returns a SoftReservationStore and instantiates the needed background informer event handlers to keep the store up to date
func NewSoftReservationStore(informer coreinformers.PodInformer) *SoftReservationStore {
	s := &SoftReservationStore{
		store: make(map[string]*SoftReservation),
	}

	informer.Informer().AddEventHandler(
		clientcache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				if pod, ok := obj.(*v1.Pod); ok {
					if _, labelFound := pod.Labels[SparkRoleLabel]; labelFound {
						return true
					}
				}
				return false
			},
			Handler: clientcache.ResourceEventHandlerFuncs{
				DeleteFunc: s.onPodDeletion,
			},
		},
	)
	return s
}

// GetSoftReservation returns a copy of the SoftReservation tied to an application if it exists (otherwise, bool returned will be false)
func (s *SoftReservationStore) GetSoftReservation(appID string) (SoftReservation, bool) {
	s.storeLock.RLock()
	defer s.storeLock.RUnlock()
	appSoftReservation, ok := s.store[appID]
	if !ok {
		return SoftReservation{}, ok
	}
	return *appSoftReservation, ok
}

// GetAllSoftReservations returns a pointer to the internal store that holds all soft reservations and should be treated as read only for now
func (s *SoftReservationStore) GetAllSoftReservations() map[string]*SoftReservation {
	s.storeLock.RLock()
	defer s.storeLock.RUnlock()
	// TODO(rkaram): consider copying the SoftReservations before returning if not a performance concern
	return s.store
}

// CreateSoftReservationIfNotExists creates an internal empty soft reservation for a particular application.
// This is a noop if the reservation already exists.
func (s *SoftReservationStore) CreateSoftReservationIfNotExists(appID string) SoftReservation {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()
	appSoftReservation, ok := s.store[appID]
	if !ok {
		r := make(map[string]v1beta1.Reservation)
		sr := &SoftReservation{
			Reservations: r,
			Status:       make(map[string]bool),
		}
		s.store[appID] = sr
		appSoftReservation = sr
	}
	return *appSoftReservation
}

// AddReservationForPod adds a reservation for an extra executor pod, attaching the associated node and resources to it.
// This is a noop if the reservation already exists.
func (s *SoftReservationStore) AddReservationForPod(ctx context.Context, appID string, podName string, reservation v1beta1.Reservation) {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()
	appSoftReservation, ok := s.store[appID]
	if !ok {
		svc1log.FromContext(ctx).Info("Could not put reservation since appID does not exist in reservation store", svc1log.SafeParam("appID", appID))
		return
	}

	if _, alreadyThere := appSoftReservation.Status[podName]; alreadyThere {
		return
	}

	appSoftReservation.Reservations[podName] = reservation
	appSoftReservation.Status[podName] = true
}

func (s *SoftReservationStore) onPodDeletion(obj interface{}) {
	ctx := context.Background()
	pod, ok := obj.(*v1.Pod)
	if !ok {
		svc1log.FromContext(ctx).Error("failed to parse object as pod")
	}
	appID := pod.Labels[SparkAppIDLabel]
	switch pod.Labels[SparkRoleLabel] {
	case Driver:
		s.removeDriverReservation(appID)
	case Executor:
		s.removeExecutorReservation(appID, pod.Name)
	}
}

func (s *SoftReservationStore) removeExecutorReservation(appID string, executorName string) {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()
	sr, found := s.store[appID]
	if !found {
		return
	}
	if _, found := sr.Reservations[executorName]; found {
		delete(sr.Reservations, executorName)
	}
	if _, found := sr.Status[executorName]; found {
		sr.Status[executorName] = false
	}
}

func (s *SoftReservationStore) removeDriverReservation(appID string) {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()
	if _, found := s.store[appID]; found {
		delete(s.store, appID)
	}
}
