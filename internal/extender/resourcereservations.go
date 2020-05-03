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

package extender

import (
	"context"
	"fmt"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/logging"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/k8s-spark-scheduler/internal/common/utils"
	"github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"k8s.io/apimachinery/pkg/labels"
	"sort"
	"sync"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var podGroupVersionKind = v1.SchemeGroupVersion.WithKind("Pod")

type ResourceReservationManager struct {
	resourceReservations *cache.ResourceReservationCache
	softReservationStore *cache.SoftReservationStore
	podLister            *SparkPodLister
	mutex 		 		 sync.RWMutex
}

func NewResourceReservationManager(
	resourceReservations *cache.ResourceReservationCache,
	softReservationStore *cache.SoftReservationStore,
	podLister            *SparkPodLister) *ResourceReservationManager  {
	return &ResourceReservationManager{
		resourceReservations: resourceReservations,
		softReservationStore: softReservationStore,
		podLister: podLister,
	}
}

func (rrm *ResourceReservationManager) GetResourceReservation(pod *v1.Pod) (*v1beta1.ResourceReservation, bool) {
	return rrm.resourceReservations.Get(pod.Namespace, pod.Labels[common.SparkAppIDLabel])
}

// CreateReservations creates the necessary reservations for an application whether those are resource reservation objects or
// in-memory soft reservations for extra executors.
func (rrm *ResourceReservationManager) CreateReservations(
	ctx context.Context,
	driver *v1.Pod,
	applicationResources *sparkApplicationResources,
	driverNode string,
	executorNodes []string) (*v1beta1.ResourceReservation, error) {
	rr, ok := rrm.GetResourceReservation(driver); if !ok {
		svc1log.FromContext(ctx).Debug("creating executor resource reservations", svc1log.SafeParams(logging.RRSafeParam(rr)))
		rr = rrm.newResourceReservation(driverNode, executorNodes, driver, applicationResources.driverResources, applicationResources.executorResources)
		err := rrm.resourceReservations.Create(rr)
		if err != nil {
			return nil, werror.WrapWithContextParams(ctx, err, "failed to create resource reservation", werror.SafeParam("reservationName", rr.Name))
		}
	}

	if applicationResources.maxExecutorCount > applicationResources.minExecutorCount {
		// only create soft reservations for applications which can request extra executors
		svc1log.FromContext(ctx).Debug("creating soft reservations for application", svc1log.SafeParam("appID", driver.Labels[common.SparkAppIDLabel]))
		rrm.softReservationStore.CreateSoftReservationIfNotExists(driver.Labels[common.SparkAppIDLabel])
	}

	return rr, nil
}

// FindAlreadyBoundReservationNode returns a node name that was previously allocated to this executor, if any.
// Binding reservations have to be idempotent. Binding the pod to the node on kube-scheduler might fail, so we want to get the same executor pod on a retry.
func (rrm *ResourceReservationManager) FindAlreadyBoundReservationNode(ctx context.Context, executor *v1.Pod) (string, error) {
	resourceReservation, ok := rrm.GetResourceReservation(executor)
	if !ok {
		return "", werror.ErrorWithContextParams(ctx, "failed to get resource reservations")
	}
	for name := range resourceReservation.Spec.Reservations {
		if resourceReservation.Status.Pods[name] == executor.Name {
			return resourceReservation.Spec.Reservations[name].Node, nil
		}
	}
	// TODO(rkaram): Check soft reservations as well
	return "", nil
}

// FindUnboundReservationNodes returns a slice of node names that have unbound reservations for this Spark application.
// This includes both reservations we have not yet scheduled any executors on as well as reservations that have executors that are now dead.
// Spark will recreate lost executors, so the replacement executors should be placed on the reserved spaces of dead executors.
func (rrm *ResourceReservationManager) FindUnboundReservationNodes(ctx context.Context, executor *v1.Pod) ([]string, error) {
	unboundReservationsToNodes, err := rrm.getUnboundReservations(ctx, executor)
	if err != nil {
		return []string{}, err
	}
	unboundReservationNodes := utils.NewStringSet(len(unboundReservationsToNodes))
	for _, node := range unboundReservationsToNodes {
		unboundReservationNodes.Add(node)
	}
	return unboundReservationNodes.ToSlice(), nil
}

func (rrm *ResourceReservationManager) GetFreeExecutorSpots(appId string) int {
	// count unbound reservations + free soft reservations
}

func (rrm *ResourceReservationManager) ReserveForExecutor(ctx context.Context, executor *v1.Pod, node string) error {
	rrm.mutex.Lock()
	defer rrm.mutex.Unlock()

	// TODO: make sure executor doesn't already have a reservation, if it does, it must be freed (we don't seem to handle this currently)

	unboundReservationsToNodes, err := rrm.getUnboundReservations(ctx, executor)
	if err != nil {
		return err
	}
	// Sort the unbound reservations such that we favor reservations that are already tied to the requested node first
	sortedUnboundReservations := make([]string, len(unboundReservationsToNodes))
	for reservation := range unboundReservationsToNodes {
		sortedUnboundReservations = append(sortedUnboundReservations, reservation)
	}
	sort.Slice(sortedUnboundReservations, func(i, j int) bool {
		return unboundReservationsToNodes[sortedUnboundReservations[i]] == node
	})

	if len(sortedUnboundReservations) > 0 {
		reservationName := sortedUnboundReservations[0]
		return rrm.bindExecutorToResourceReservation(ctx, executor, reservationName, unboundReservationsToNodes[reservationName])
	}

	// Try to get a soft reservation if it is a dynamic allocation application
	if rrm.getFreeExtraExecutorSpots(executor) > 0 {
		return rrm.bindExecutorToSoftReservation(ctx, executor, node)
	}

	return werror.ErrorWithContextParams(ctx, "failed to find free reservation for executor")
}

func (rrm *ResourceReservationManager) bindExecutorToResourceReservation(ctx context.Context, executor *v1.Pod, reservationName string, node string) error {
	resourceReservation, ok := rrm.GetResourceReservation(executor)
	if !ok {
		return werror.ErrorWithContextParams(ctx,"failed to get resource reservationName")
	}
	copyResourceReservation := resourceReservation.DeepCopy()
	reservationObject := copyResourceReservation.Spec.Reservations[reservationName]
	reservationObject.Node = node
	copyResourceReservation.Spec.Reservations[reservationName] = reservationObject
	copyResourceReservation.Status.Pods[reservationName] = executor.Name
	err := rrm.resourceReservations.Update(copyResourceReservation)
	if err != nil {
		return werror.WrapWithContextParams(ctx, err, "failed to update resource reservationName")
	}
	return nil
}

func (rrm *ResourceReservationManager) bindExecutorToSoftReservation(ctx context.Context, executor *v1.Pod, node string) error {
	driver, err := rrm.podLister.getDriverPod(ctx, executor)
	if err != nil {
		return err
	}
	sparkResources, err := sparkResources(ctx, driver)
	if err != nil {
		return err
	}
	softReservation := v1beta1.Reservation{
		Node:   node,
		CPU:    sparkResources.executorResources.CPU,
		Memory: sparkResources.executorResources.Memory,
	}
	return rrm.softReservationStore.AddReservationForPod(ctx, driver.Labels[common.SparkAppIDLabel], executor.Name, softReservation)
}

func (rrm *ResourceReservationManager) getUnboundReservations(ctx context.Context, executor *v1.Pod) (map[string]string, error) {
	resourceReservation, ok := rrm.GetResourceReservation(executor)
	if !ok {
		return nil, werror.ErrorWithContextParams(ctx,"failed to get resource reservation")
	}
	activePodNames, err := rrm.getActivePodNames(ctx, executor)
	if err != nil {
		return nil, err
	}

	unboundReservationsToNodes := make(map[string]string, len(resourceReservation.Spec.Reservations))
	for reservationName, reservation := range resourceReservation.Spec.Reservations {
		podIdentifier, ok := resourceReservation.Status.Pods[reservationName]
		if !ok || !activePodNames[podIdentifier] {
			unboundReservationsToNodes[reservationName] = reservation.Node
		}
	}
	return unboundReservationsToNodes, nil
}

func (rrm *ResourceReservationManager) getFreeExtraExecutorSpots(executor *v1.Pod) int {
	// count free soft reservations
}

// getActivePodNames returns a map of pod names that are still active in the passed pod's namespace
func (rrm *ResourceReservationManager) getActivePodNames(ctx context.Context, pod *v1.Pod) (map[string]bool, error) {
	selector := labels.Set(map[string]string{common.SparkAppIDLabel: pod.Labels[common.SparkAppIDLabel]}).AsSelector()
	pods, err := rrm.podLister.Pods(pod.Namespace).List(selector)
	if err != nil {
		return nil, werror.WrapWithContextParams(ctx, err, "failed to list pods")
	}
	activePodNames := make(map[string]bool, len(pods))
	for _, pod := range pods {
		if !rrm.isPodTerminated(pod) {
			activePodNames[pod.Name] = true
		}
	}
	return activePodNames, nil
}

func (rrm *ResourceReservationManager) isPodTerminated(pod *v1.Pod) bool {
	allTerminated := len(pod.Status.ContainerStatuses) > 0
	for _, status := range pod.Status.ContainerStatuses {
		allTerminated = allTerminated && status.State.Terminated != nil
	}
	return allTerminated
}

func (rrm *ResourceReservationManager) newResourceReservation(driverNode string, executorNodes []string, driver *v1.Pod, driverResources, executorResources *resources.Resources) *v1beta1.ResourceReservation {
	reservations := make(map[string]v1beta1.Reservation, len(executorNodes)+1)
	reservations["driver"] = v1beta1.Reservation{
		Node:   driverNode,
		CPU:    driverResources.CPU,
		Memory: driverResources.Memory,
	}
	for idx, nodeName := range executorNodes {
		reservations[rrm.executorReservationName(idx)] = v1beta1.Reservation{
			Node:   nodeName,
			CPU:    executorResources.CPU,
			Memory: executorResources.Memory,
		}
	}
	return &v1beta1.ResourceReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name:            driver.Labels[common.SparkAppIDLabel],
			Namespace:       driver.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(driver, podGroupVersionKind)},
			Labels: map[string]string{
				v1beta1.AppIDLabel: driver.Labels[common.SparkAppIDLabel],
			},
		},
		Spec: v1beta1.ResourceReservationSpec{
			Reservations: reservations,
		},
		Status: v1beta1.ResourceReservationStatus{
			Pods: map[string]string{"driver": driver.Name},
		},
	}
}

func (rrm *ResourceReservationManager) executorReservationName(i int) string {
	return fmt.Sprintf("executor-%d", i+1)
}
