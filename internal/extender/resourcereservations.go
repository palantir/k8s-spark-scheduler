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
	"math"
	"sync"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/logging"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	"github.com/palantir/k8s-spark-scheduler/internal/common/utils"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

var podGroupVersionKind = v1.SchemeGroupVersion.WithKind("Pod")

// ResourceReservationManager is a central point which manages the creation and reading of both resource reservations and soft reservations
type ResourceReservationManager struct {
	resourceReservations *cache.ResourceReservationCache
	softReservationStore *cache.SoftReservationStore
	podLister            *SparkPodLister
	mutex                sync.RWMutex
}

// NewResourceReservationManager creates and returns a ResourceReservationManager
func NewResourceReservationManager(
	resourceReservations *cache.ResourceReservationCache,
	softReservationStore *cache.SoftReservationStore,
	podLister *SparkPodLister) *ResourceReservationManager {
	return &ResourceReservationManager{
		resourceReservations: resourceReservations,
		softReservationStore: softReservationStore,
		podLister:            podLister,
	}
}

// GetResourceReservation returns the resource reservation for the passed pod, if any.
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
	rr, ok := rrm.GetResourceReservation(driver)
	if !ok {
		rr = newResourceReservation(driverNode, executorNodes, driver, applicationResources.driverResources, applicationResources.executorResources)
		svc1log.FromContext(ctx).Debug("creating executor resource reservations", svc1log.SafeParams(logging.RRSafeParam(rr)))
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

// FindAlreadyBoundReservationNode returns a node name that was previously allocated to this executor if any, or false otherwise.
// Binding reservations have to be idempotent. Binding the pod to the node on kube-scheduler might fail, so we want to get the same executor pod on a retry.
func (rrm *ResourceReservationManager) FindAlreadyBoundReservationNode(ctx context.Context, executor *v1.Pod) (string, bool, error) {
	resourceReservation, ok := rrm.GetResourceReservation(executor)
	if !ok {
		return "", false, werror.ErrorWithContextParams(ctx, "failed to get resource reservations")
	}
	for name := range resourceReservation.Spec.Reservations {
		if resourceReservation.Status.Pods[name] == executor.Name {
			return resourceReservation.Spec.Reservations[name].Node, true, nil
		}
	}

	// Check any soft reservations as well
	if sr, ok := rrm.softReservationStore.GetExecutorSoftReservation(ctx, executor); ok {
		return sr.Node, true, nil
	}
	return "", false, nil
}

// FindUnboundReservationNodes returns a slice of node names that have unbound reservations for this Spark application.
// This includes both reservations we have not yet scheduled any executors on as well as reservations that have executors that are now dead.
// Spark will recreate lost executors, so the replacement executors should be placed on the reserved spaces of dead executors.
func (rrm *ResourceReservationManager) FindUnboundReservationNodes(ctx context.Context, executor *v1.Pod) ([]string, bool, error) {
	unboundReservationsToNodes, err := rrm.getUnboundReservations(ctx, executor)
	if err != nil {
		return []string{}, false, err
	}
	unboundReservationNodes := utils.NewStringSet(len(unboundReservationsToNodes))
	found := false
	for _, node := range unboundReservationsToNodes {
		unboundReservationNodes.Add(node)
		found = true
	}
	return unboundReservationNodes.ToSlice(), found, nil
}

// GetRemainingAllowedExecutorCount returns the number of executors the application can still schedule.
func (rrm *ResourceReservationManager) GetRemainingAllowedExecutorCount(ctx context.Context, executor *v1.Pod) (int, error) {
	unboundReservations, err := rrm.getUnboundReservations(ctx, executor)
	if err != nil {
		return 0, err
	}
	softReservationFreeSpots, err := rrm.getFreeSoftReservationSpots(ctx, executor)
	if err != nil {
		return 0, err
	}
	return len(unboundReservations) + softReservationFreeSpots, nil
}

// ReserveForExecutorOnUnboundReservation binds a resource reservation already tied to the passed node to the executor.
// This will only succeed if there are unbound reservations on that node.
func (rrm *ResourceReservationManager) ReserveForExecutorOnUnboundReservation(ctx context.Context, executor *v1.Pod, node string) error {
	rrm.mutex.Lock()
	defer rrm.mutex.Unlock()

	unboundReservationsToNodes, err := rrm.getUnboundReservations(ctx, executor)
	if err != nil {
		return err
	}
	for reservationName, reservationNode := range unboundReservationsToNodes {
		if reservationNode == node {
			return rrm.bindExecutorToResourceReservation(ctx, executor, reservationName, node)
		}
	}

	return werror.ErrorWithContextParams(ctx, "failed to find free reservation on requested node for executor")
}

// ReserveForExecutorOnRescheduledNode creates a reservation for the passed executor on the passed node by replacing another unbound reservation.
// This reservation could either be a resource reservation, or a soft reservation if dynamic allocation is enabled.
func (rrm *ResourceReservationManager) ReserveForExecutorOnRescheduledNode(ctx context.Context, executor *v1.Pod, node string) error {
	rrm.mutex.Lock()
	defer rrm.mutex.Unlock()

	unboundReservationsToNodes, err := rrm.getUnboundReservations(ctx, executor)
	if err != nil {
		return err
	}

	if len(unboundReservationsToNodes) > 0 {
		return rrm.bindExecutorToResourceReservation(ctx, executor, getAKeyFromMap(unboundReservationsToNodes), node)
	}

	// Try to get a soft reservation if it is a dynamic allocation application
	extraExecutorFreeSpots, err := rrm.getFreeSoftReservationSpots(ctx, executor)
	if err != nil {
		return werror.WrapWithContextParams(ctx, err, "failed to count free extra executor spots remaining")
	}
	if extraExecutorFreeSpots > 0 {
		return rrm.bindExecutorToSoftReservation(ctx, executor, node)
	}

	return werror.ErrorWithContextParams(ctx, "failed to find free reservation for executor")
}

// GetReservedResources returns the resources per node that are reserved for executors.
func (rrm *ResourceReservationManager) GetReservedResources() resources.NodeGroupResources {
	resourceReservations := rrm.resourceReservations.List()
	usage := resources.UsageForNodes(resourceReservations)
	usage.Add(rrm.softReservationStore.UsedSoftReservationResources())
	return usage
}

func (rrm *ResourceReservationManager) bindExecutorToResourceReservation(ctx context.Context, executor *v1.Pod, reservationName string, node string) error {
	resourceReservation, ok := rrm.GetResourceReservation(executor)
	if !ok {
		return werror.ErrorWithContextParams(ctx, "failed to get resource reservationName", werror.SafeParam("reservationName", reservationName))
	}
	copyResourceReservation := resourceReservation.DeepCopy()
	reservationObject := copyResourceReservation.Spec.Reservations[reservationName]
	reservationObject.Node = node
	copyResourceReservation.Spec.Reservations[reservationName] = reservationObject
	copyResourceReservation.Status.Pods[reservationName] = executor.Name
	err := rrm.resourceReservations.Update(copyResourceReservation)
	if err != nil {
		return werror.WrapWithContextParams(ctx, err, "failed to update resource reservationName", werror.SafeParam("reservationName", reservationName))
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

// getUnboundReservations returns a map of reservationName to node for all reservations that are either not bound to an executor,
// bound to a now-dead executor, or bound to an executor that has now been scheduled onto another node
func (rrm *ResourceReservationManager) getUnboundReservations(ctx context.Context, executor *v1.Pod) (map[string]string, error) {
	resourceReservation, ok := rrm.GetResourceReservation(executor)
	if !ok {
		return nil, werror.ErrorWithContextParams(ctx, "failed to get resource reservation")
	}
	activePodNames, err := rrm.getActivePods(ctx, executor)
	if err != nil {
		return nil, err
	}

	unboundReservationsToNodes := make(map[string]string, len(resourceReservation.Spec.Reservations))
	for reservationName, reservation := range resourceReservation.Spec.Reservations {
		podIdentifier, ok := resourceReservation.Status.Pods[reservationName]
		pod, isActivePod := activePodNames[podIdentifier]
		if !ok || !isActivePod || pod.Spec.NodeName != reservation.Node {
			unboundReservationsToNodes[reservationName] = reservation.Node
		}
	}
	return unboundReservationsToNodes, nil
}

func (rrm *ResourceReservationManager) getFreeSoftReservationSpots(ctx context.Context, executor *v1.Pod) (int, error) {
	usedSoftReservationCount := 0
	sr, ok := rrm.softReservationStore.GetSoftReservation(executor.Labels[common.SparkAppIDLabel])
	if !ok {
		return 0, nil
	}
	usedSoftReservationCount = len(sr.Reservations)
	driver, err := rrm.podLister.getDriverPod(ctx, executor)
	if err != nil {
		return 0, err
	}
	sparkResources, err := sparkResources(ctx, driver)
	if err != nil {
		return 0, err
	}
	maxAllowedExtraExecutors := sparkResources.maxExecutorCount - sparkResources.minExecutorCount
	return int(math.Max(float64(maxAllowedExtraExecutors-usedSoftReservationCount), 0)), nil
}

// getActivePods returns a map of pod names to pods that are still active in the passed pod's namespace
func (rrm *ResourceReservationManager) getActivePods(ctx context.Context, pod *v1.Pod) (map[string]*v1.Pod, error) {
	selector := labels.Set(map[string]string{common.SparkAppIDLabel: pod.Labels[common.SparkAppIDLabel]}).AsSelector()
	pods, err := rrm.podLister.Pods(pod.Namespace).List(selector)
	if err != nil {
		return nil, werror.WrapWithContextParams(ctx, err, "failed to list pods")
	}
	activePodNames := make(map[string]*v1.Pod, len(pods))
	for _, pod := range pods {
		if !utils.IsPodTerminated(pod) {
			activePodNames[pod.Name] = pod
		}
	}
	return activePodNames, nil
}

// newResourceReservation builds a reservation object with the pods and resources passed and returns it.
func newResourceReservation(driverNode string, executorNodes []string, driver *v1.Pod, driverResources, executorResources *resources.Resources) *v1beta1.ResourceReservation {
	reservations := make(map[string]v1beta1.Reservation, len(executorNodes)+1)
	reservations["driver"] = v1beta1.Reservation{
		Node:   driverNode,
		CPU:    driverResources.CPU,
		Memory: driverResources.Memory,
	}
	for idx, nodeName := range executorNodes {
		reservations[executorReservationName(idx)] = v1beta1.Reservation{
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

// executorReservationName returns a string following the convention of calling executor reservations in increments
func executorReservationName(i int) string {
	return fmt.Sprintf("executor-%d", i+1)
}

func getAKeyFromMap(input map[string]string) string {
	for key := range input {
		return key
	}
	return ""
}
