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
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	"github.com/palantir/k8s-spark-scheduler/internal/common/utils"
	"sync"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientcache "k8s.io/client-go/tools/cache"
)

// SoftReservationStore is an in-memory store that keeps track of soft reservations granted to extra executors for applications that support dynamic allocation
type SoftReservationStore struct {
	store     map[string]*SoftReservation // SparkAppID -> SoftReservation
	storeLock sync.RWMutex
	logger    svc1log.Logger
}

// SoftReservation is an in-memory reservation for a particular spark application that keeps track of extra executors allocated over the
// min reservation count
type SoftReservation struct {
	// Executor pod name -> Reservation (only valid ones here)
	Reservations map[string]v1beta1.Reservation

	// Executor pod name -> Reservation valid or not
	// The reason for this is that we want to keep a history of previously allocated extra executors that we should not create a
	// Reservation for if we already have in the past even if the executor is now dead. This prevents the scenario where we have a race between
	// the executor death event handling and the executor's scheduling event.
	Status map[string]bool
}

// NewSoftReservationStore builds and returns a SoftReservationStore and instantiates the needed background informer event handlers to keep the store up to date.
func NewSoftReservationStore(ctx context.Context, informer coreinformers.PodInformer) *SoftReservationStore {
	s := &SoftReservationStore{
		store:  make(map[string]*SoftReservation),
		logger: svc1log.FromContext(ctx),
	}

	informer.Informer().AddEventHandler(
		clientcache.FilteringResourceEventHandler{
			FilterFunc: utils.IsSparkSchedulerPod,
			Handler: clientcache.ResourceEventHandlerFuncs{
				DeleteFunc: s.onPodDeletion,
			},
		},
	)
	return s
}

// GetSoftReservation returns a copy of the SoftReservation tied to an application if it exists (otherwise, bool returned will be false).
func (s *SoftReservationStore) GetSoftReservation(appID string) (*SoftReservation, bool) {
	s.storeLock.RLock()
	defer s.storeLock.RUnlock()
	appSoftReservation, ok := s.store[appID]
	if !ok {
		return &SoftReservation{}, ok
	}
	return s.deepCopySoftReservation(appSoftReservation), ok
}

// GetAllSoftReservationsCopy returns a copy of the internal store. As this indicates, this method does a deep copy
// which is slow and should only be used for purposes where this is acceptable such as tests.
func (s *SoftReservationStore) GetAllSoftReservationsCopy() map[string]*SoftReservation {
	s.storeLock.RLock()
	defer s.storeLock.RUnlock()
	storeCopy := make(map[string]*SoftReservation, len(s.store))
	for appID, sr := range s.store {
		storeCopy[appID] = s.deepCopySoftReservation(sr)
	}
	return storeCopy
}

// CreateSoftReservationIfNotExists creates an internal empty soft reservation for a particular application.
// This is a noop if the reservation already exists.
func (s *SoftReservationStore) CreateSoftReservationIfNotExists(appID string) {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()
	_, ok := s.store[appID]
	if !ok {
		r := make(map[string]v1beta1.Reservation)
		sr := &SoftReservation{
			Reservations: r,
			Status:       make(map[string]bool),
		}
		s.store[appID] = sr
	}
}

// AddReservationForPod adds a reservation for an extra executor pod, attaching the associated node and resources to it.
// This is a noop if the reservation already exists.
func (s *SoftReservationStore) AddReservationForPod(ctx context.Context, appID string, podName string, reservation v1beta1.Reservation) error {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()
	appSoftReservation, ok := s.store[appID]
	if !ok {
		return werror.Error("Could not add soft reservation since appID does not exist in reservation store",
			werror.SafeParam("appID", appID))
	}

	if _, alreadyThere := appSoftReservation.Status[podName]; alreadyThere {
		return nil
	}

	appSoftReservation.Reservations[podName] = reservation
	appSoftReservation.Status[podName] = true
	return nil
}

// ExecutorHasSoftReservation returns true when the passed executor pod currently has a SoftReservation, false otherwise.
func (s *SoftReservationStore) ExecutorHasSoftReservation(ctx context.Context, executor *v1.Pod) bool {
	s.storeLock.RLock()
	defer s.storeLock.RUnlock()
	appID, ok := executor.Labels[common.SparkAppIDLabel]
	if !ok {
		svc1log.FromContext(ctx).Error("Cannot get SoftReservation for pod which does not have application ID label set",
			svc1log.SafeParam("podName", executor.Name),
			svc1log.SafeParam("expectedLabel", common.SparkAppIDLabel))
		return false
	}
	if sr, ok := s.store[appID]; ok {
		_, ok := sr.Reservations[executor.Name]
		return ok
	}
	return false
}

// UsedSoftReservationResources returns SoftReservation usage by node.
func (s *SoftReservationStore) UsedSoftReservationResources() resources.NodeGroupResources {
	s.storeLock.RLock()
	defer s.storeLock.RUnlock()
	res := resources.NodeGroupResources(map[string]*resources.Resources{})

	for _, softReservation := range s.store {
		for _, reservationObject := range softReservation.Reservations {
			node := reservationObject.Node
			if res[node] == nil {
				res[node] = resources.Zero()
			}
			res[node].AddFromReservation(&reservationObject)
		}
	}
	return res
}

func (s *SoftReservationStore) onPodDeletion(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		s.logger.Warn("failed to parse object as pod, trying to get from tombstone")
		tombstone, ok := obj.(clientcache.DeletedFinalStateUnknown)
		if !ok {
			s.logger.Error("failed to get object from tombstone")
			return
		}
		pod, ok = tombstone.Obj.(*v1.Pod)
		if !ok {
			s.logger.Error("failed to get pod from tombstone")
			return
		}
	}
	appID := pod.Labels[common.SparkAppIDLabel]
	switch pod.Labels[common.SparkRoleLabel] {
	case common.Driver:
		s.removeDriverReservation(appID)
	case common.Executor:
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
	// We always mark this as false to remember that we saw the executor die, and prevent a race between this death event
	// and the request to schedule the executor
	sr.Status[executorName] = false
}

func (s *SoftReservationStore) removeDriverReservation(appID string) {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()
	if _, found := s.store[appID]; found {
		delete(s.store, appID)
	}
}

func (s *SoftReservationStore) deepCopySoftReservation(reservation *SoftReservation) *SoftReservation {
	reservationsCopy := make(map[string]v1beta1.Reservation, len(reservation.Reservations))
	for name, res := range reservation.Reservations {
		reservationsCopy[name] = *res.DeepCopy()
	}
	statusCopy := make(map[string]bool, len(reservation.Status))
	for name, status := range reservation.Status {
		statusCopy[name] = status
	}
	return &SoftReservation{
		Reservations: reservationsCopy,
		Status:       statusCopy,
	}
}

// Metric related methods

// GetApplicationCount returns the distinct number of applications that are tracked in the SoftReservationStore (whether they currently have extra executors or not)
func (s *SoftReservationStore) GetApplicationCount() int {
	s.storeLock.RLock()
	defer s.storeLock.RUnlock()
	return len(s.store)
}

// GetActiveExtraExecutorCount returns the total number of extra executors that are currently allocated and have SoftReservations in the SoftReservationStore
// (excluding the ones that are already marked as dead by the store)
func (s *SoftReservationStore) GetActiveExtraExecutorCount() int {
	s.storeLock.RLock()
	defer s.storeLock.RUnlock()
	var executorCount = 0
	for _, sr := range s.store {
		executorCount += len(sr.Reservations)
	}
	return executorCount
}
