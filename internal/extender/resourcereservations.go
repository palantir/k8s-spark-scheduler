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
	"github.com/palantir/k8s-spark-scheduler/internal/reservations"
	"github.com/palantir/k8s-spark-scheduler/internal/types"
	"math"
	"sync"
	"time"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta2"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/logging"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	"github.com/palantir/k8s-spark-scheduler/internal/common/utils"
	"github.com/palantir/k8s-spark-scheduler/internal/metrics"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientcache "k8s.io/client-go/tools/cache"
)

var podGroupVersionKind = v1.SchemeGroupVersion.WithKind("Pod")

// ResourceReservationManager is a central point which manages the creation and reading of both resource reservations and soft reservations
type ResourceReservationManager struct {
	resourceReservations                 reservations.Store
	softReservationStore                 *cache.SoftReservationStore
	podLister                            *SparkPodLister
	mutex                                sync.Mutex
	dynamicAllocationCompactionApps      map[string]string
	dynamicAllocationCompactionSliceLock sync.Mutex
	context                              context.Context
}

// NewResourceReservationManager creates and returns a ResourceReservationManager
func NewResourceReservationManager(
	ctx context.Context,
	resourceReservations reservations.Store,
	softReservationStore *cache.SoftReservationStore,
	podLister *SparkPodLister,
	informer coreinformers.PodInformer) ResourceReservationManager {
	rrm := &ResourceReservationManager{
		resourceReservations: resourceReservations,
		softReservationStore: softReservationStore,
		podLister:            podLister,
		context:              ctx,
	}

	informer.Informer().AddEventHandler(
		clientcache.FilteringResourceEventHandler{
			FilterFunc: utils.IsSparkSchedulerExecutorPod,
			Handler: clientcache.ResourceEventHandlerFuncs{
				DeleteFunc: rrm.onExecutorPodDeletion,
			},
		},
	)

	return rrm
}

// GetResourceReservation returns the resource reservation for the passed pod, if any.
func (rrm *ResourceReservationManager) GetResourceReservation(ctx context.Context, appID string, namespace string) (*v1beta2.ResourceReservation, bool) {
	r, err := rrm.resourceReservations.Get(ctx, namespace, appID)
	if err != nil {
		return nil, false
	}
	return r, true
}

// GetSoftResourceReservation returns the soft resource reservation for this appId
func (rrm *ResourceReservationManager) GetSoftResourceReservation(appID string) (*cache.SoftReservation, bool) {
	return rrm.softReservationStore.GetSoftReservation(appID)
}

// PodHasReservation returns if the passed pod has any reservation whether it is a resource reservation or a soft reservation
func (rrm *ResourceReservationManager) PodHasReservation(ctx context.Context, pod *v1.Pod) bool {
	appID, ok := pod.Labels[common.SparkAppIDLabel]
	if !ok {
		return false
	}
	if rr, ok := rrm.GetResourceReservation(ctx, appID, pod.Namespace); ok {
		for _, rPodName := range rr.Status.Pods {
			if pod.Name == rPodName {
				return true
			}
		}
	}
	if pod.Labels[common.SparkRoleLabel] == common.Executor && rrm.softReservationStore.ExecutorHasSoftReservation(ctx, pod) {
		return true
	}

	return false
}

// CreateReservations creates the necessary reservations for an application whether those are resource reservation objects or
// in-memory soft reservations for extra executors.
func (rrm *ResourceReservationManager) CreateReservations(
	ctx context.Context,
	driver *v1.Pod,
	applicationResources *types.SparkApplicationResources,
	driverNode string,
	executorNodes []string) (*v1beta2.ResourceReservation, error) {
	rr, ok := rrm.GetResourceReservation(ctx, driver.Labels[common.SparkAppIDLabel], driver.Namespace)
	if !ok {
		rr = newResourceReservation(driverNode, executorNodes, driver, applicationResources.DriverResources, applicationResources.ExecutorResources)
		svc1log.FromContext(ctx).Debug("creating executor resource reservations", svc1log.SafeParams(logging.RRSafeParamV1Beta2(rr)))
		err := rrm.resourceReservations.Create(ctx, rr)
		if err != nil {
			return nil, werror.WrapWithContextParams(ctx, err, "failed to create resource reservation", werror.SafeParam("reservationName", rr.Name))
		}
	}

	if applicationResources.MaxExecutorCount > applicationResources.MinExecutorCount {
		// only create soft reservations for applications which can request extra executors
		svc1log.FromContext(ctx).Debug("creating soft reservations for application", svc1log.SafeParam("appID", driver.Labels[common.SparkAppIDLabel]))
		rrm.softReservationStore.CreateSoftReservationIfNotExists(driver.Labels[common.SparkAppIDLabel])
	}

	return rr, nil
}

// FindAlreadyBoundReservationNode returns a node name that was previously allocated to this executor if any, or false otherwise.
// Binding reservations have to be idempotent. Binding the pod to the node on kube-scheduler might fail, so we want to get the same executor pod on a retry.
func (rrm *ResourceReservationManager) FindAlreadyBoundReservationNode(ctx context.Context, executor *v1.Pod) (string, bool, error) {
	resourceReservation, ok := rrm.GetResourceReservation(ctx, executor.Labels[common.SparkAppIDLabel], executor.Namespace)
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
	unboundReservationsToNodes, err := rrm.getUnboundReservations(ctx, executor.Labels[common.SparkAppIDLabel], executor.Namespace)
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
func (rrm *ResourceReservationManager) GetRemainingAllowedExecutorCount(ctx context.Context, appID string, namespace string) (int, error) {
	unboundReservations, err := rrm.getUnboundReservations(ctx, appID, namespace)
	if err != nil {
		return 0, err
	}
	softReservationFreeSpots, err := rrm.getFreeSoftReservationSpots(ctx, appID, namespace)
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

	unboundReservationsToNodes, err := rrm.getUnboundReservations(ctx, executor.Labels[common.SparkAppIDLabel], executor.Namespace)
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

	unboundReservationsToNodes, err := rrm.getUnboundReservations(ctx, executor.Labels[common.SparkAppIDLabel], executor.Namespace)
	if err != nil {
		return err
	}

	if len(unboundReservationsToNodes) > 0 {
		return rrm.bindExecutorToResourceReservation(ctx, executor, getAKeyFromMap(unboundReservationsToNodes), node)
	}

	// Try to get a soft reservation if it is a dynamic allocation application
	extraExecutorFreeSpots, err := rrm.getFreeSoftReservationSpots(ctx, executor.Labels[common.SparkAppIDLabel], executor.Namespace)
	if err != nil {
		return werror.WrapWithContextParams(ctx, err, "failed to count free extra executor spots remaining")
	}
	if extraExecutorFreeSpots > 0 {
		return rrm.bindExecutorToSoftReservation(ctx, executor, node)
	}

	return werror.ErrorWithContextParams(ctx, "failed to find free reservation for executor")
}

// GetReservedResources returns the resources per node that are reserved for executors.
func (rrm *ResourceReservationManager) GetReservedResources(ctx context.Context) resources.NodeGroupResources {
	resourceReservations, _ := rrm.resourceReservations.List(ctx)
	usage := resources.UsageForNodes(resourceReservations)
	usage.Add(rrm.softReservationStore.UsedSoftReservationResources())
	return usage
}

// CompactDynamicAllocationApplications compacts reservations for executors belonging to dynamic allocation applications by moving
// any soft reservations to resource reservations occupied by now-dead executors. This ensures we have relatively up to date resource
// reservation objects and report correctly on reserved usage.
func (rrm *ResourceReservationManager) CompactDynamicAllocationApplications(ctx context.Context) {
	timer := metrics.GetAndStartSoftReservationCompactionTimer()
	defer timer.MarkCompactionComplete(ctx)
	dynamicAllocationAppsToCompact := rrm.drainDynamicAllocationCompactionApps()

	rrm.mutex.Lock()
	defer rrm.mutex.Unlock()
	for appID, appNamespace := range dynamicAllocationAppsToCompact {
		sr, ok := rrm.softReservationStore.GetSoftReservation(appID)
		if !ok {
			// this application no longer has soft reservations
			continue
		}
		svc1log.FromContext(rrm.context).Info("starting executor compaction for application", svc1log.SafeParam("appID", appID))
		pods, err := rrm.getActivePods(ctx, appNamespace, appID)
		if err != nil {
			svc1log.FromContext(rrm.context).Error("error getting active pods during compaction", svc1log.SafeParam("podNamespace", appNamespace), svc1log.Stacktrace(err))
			continue
		}
		for podName := range sr.Reservations {
			pod, ok := pods[podName]
			if !ok {
				svc1log.FromContext(rrm.context).Info("executor pod with soft reservation no longer active, skipping compaction for this one",
					svc1log.SafeParam("podNamespace", pod.Namespace),
					svc1log.SafeParam("podName", podName))
				continue
			}
			rrm.compactSoftReservationPod(ctx, pod)
		}
	}
}

// compactSoftReservationPod moves a single pod's soft reservation to a resource reservation if there is an unbound one.
// Note that this is a helper method and is assumed to have been called inside a lock of the rrm.mutex
func (rrm *ResourceReservationManager) compactSoftReservationPod(ctx context.Context, pod *v1.Pod) {
	appID := pod.Labels[common.SparkAppIDLabel]
	unboundReservationsToNodes, err := rrm.getUnboundReservations(ctx, appID, pod.Namespace)
	if err != nil {
		svc1log.FromContext(rrm.context).Error("failed to get unbound reservations for executor", svc1log.SafeParam("podNamespace", pod.Namespace),
			svc1log.SafeParam("podName", pod.Name), svc1log.Stacktrace(err))
		return
	}
	if len(unboundReservationsToNodes) > 0 {
		for reservationName, reservationNode := range unboundReservationsToNodes {
			if reservationNode == pod.Spec.NodeName {
				svc1log.FromContext(rrm.context).Info("compacting executor soft reservation to resource reservation",
					svc1log.SafeParam("podNamespace", pod.Namespace), svc1log.SafeParam("podName", pod.Name),
					svc1log.SafeParam("nodeName", reservationNode), svc1log.SafeParam("reservationName", reservationName))
				err := rrm.bindExecutorToResourceReservation(ctx, pod, reservationName, reservationNode)
				if err != nil {
					svc1log.FromContext(rrm.context).Error("failed to compact soft reservation to same node resource reservation",
						svc1log.SafeParam("nodeName", reservationNode), svc1log.Stacktrace(err))
					return
				}
				rrm.softReservationStore.RemoveExecutorReservation(appID, pod.Name)
				return
			}
		}
		reservationName := getAKeyFromMap(unboundReservationsToNodes)
		err := rrm.bindExecutorToResourceReservation(ctx, pod, reservationName, unboundReservationsToNodes[reservationName])
		if err != nil {
			svc1log.FromContext(rrm.context).Error("failed to compact soft reservation to different node resource reservation",
				svc1log.SafeParam("podNamespace", pod.Namespace), svc1log.SafeParam("podName", pod.Name),
				svc1log.SafeParam("nodeName", unboundReservationsToNodes[reservationName]), svc1log.Stacktrace(err))
			return
		}
		rrm.softReservationStore.RemoveExecutorReservation(appID, pod.Name)
	}
}

func (rrm *ResourceReservationManager) drainDynamicAllocationCompactionApps() map[string]string {
	rrm.dynamicAllocationCompactionSliceLock.Lock()
	defer rrm.dynamicAllocationCompactionSliceLock.Unlock()
	dynamicAllocationCompactionDrain := make(map[string]string, len(rrm.dynamicAllocationCompactionApps))
	for appID, namespace := range rrm.dynamicAllocationCompactionApps {
		dynamicAllocationCompactionDrain[appID] = namespace
	}
	rrm.dynamicAllocationCompactionApps = make(map[string]string, len(dynamicAllocationCompactionDrain))
	return dynamicAllocationCompactionDrain
}

func (rrm *ResourceReservationManager) bindExecutorToResourceReservation(ctx context.Context, executor *v1.Pod, reservationName string, node string) error {
	resourceReservation, ok := rrm.GetResourceReservation(ctx, executor.Labels[common.SparkAppIDLabel], executor.Namespace)
	if !ok {
		return werror.ErrorWithContextParams(ctx, "failed to get resource reservationName", werror.SafeParam("reservationName", reservationName))
	}
	copyResourceReservation := resourceReservation.DeepCopy()
	reservationObject := copyResourceReservation.Spec.Reservations[reservationName]
	reservationObject.Node = node
	copyResourceReservation.Spec.Reservations[reservationName] = reservationObject
	copyResourceReservation.Status.Pods[reservationName] = executor.Name
	err := rrm.resourceReservations.Update(ctx, copyResourceReservation)
	if err != nil {
		return werror.WrapWithContextParams(ctx, err, "failed to update resource reservationName", werror.SafeParam("reservationName", reservationName))
	}

	// only report the metric the first time the reservation is bound
	if _, ok := resourceReservation.Status.Pods[reservationName]; !ok {
		// this is the k8s server time, so the duration we're computing only makes sense if clocks are reasonably kept in sync
		creationTime := resourceReservation.CreationTimestamp.Time
		duration := time.Now().Sub(creationTime)
		metrics.ReportTimeToFirstBindMetrics(ctx, duration)
	}
	return nil
}

func (rrm *ResourceReservationManager) bindExecutorToSoftReservation(ctx context.Context, executor *v1.Pod, node string) error {
	driver, err := rrm.podLister.getDriverPodForExecutor(ctx, executor)
	if err != nil {
		return err
	}
	sparkResources, err := sparkResources(ctx, driver)
	if err != nil {
		return err
	}
	softReservation := v1beta2.Reservation{
		Node: node,
		Resources: v1beta2.ResourceList{
			string(v1beta2.ResourceCPU):       &sparkResources.ExecutorResources.CPU,
			string(v1beta2.ResourceMemory):    &sparkResources.ExecutorResources.Memory,
			string(v1beta2.ResourceNvidiaGPU): &sparkResources.ExecutorResources.NvidiaGPU,
		},
	}
	return rrm.softReservationStore.AddReservationForPod(ctx, driver.Labels[common.SparkAppIDLabel], executor.Name, softReservation)
}

// getUnboundReservations returns a map of reservationName to node for all reservations that are either not bound to an executor,
// bound to a now-dead executor, or bound to an executor that has now been scheduled onto another node
func (rrm *ResourceReservationManager) getUnboundReservations(ctx context.Context, appID string, namespace string) (map[string]string, error) {
	resourceReservation, ok := rrm.GetResourceReservation(ctx, appID, namespace)
	if !ok {
		return nil, werror.ErrorWithContextParams(ctx, "failed to get resource reservation")
	}
	activePodNames, err := rrm.getActivePods(ctx, appID, namespace)
	if err != nil {
		return nil, err
	}

	unboundReservationsToNodes := make(map[string]string, len(resourceReservation.Spec.Reservations))
	for reservationName, reservation := range resourceReservation.Spec.Reservations {
		podIdentifier, ok := resourceReservation.Status.Pods[reservationName]
		pod, isActivePod := activePodNames[podIdentifier]
		if !ok || !isActivePod || (pod.Spec.NodeName != "" && pod.Spec.NodeName != reservation.Node) {
			unboundReservationsToNodes[reservationName] = reservation.Node
		}
	}
	return unboundReservationsToNodes, nil
}

func (rrm *ResourceReservationManager) getFreeSoftReservationSpots(ctx context.Context, appID string, namespace string) (int, error) {
	usedSoftReservationCount := 0
	sr, ok := rrm.softReservationStore.GetSoftReservation(appID)
	if !ok {
		return 0, nil
	}
	usedSoftReservationCount = len(sr.Reservations)
	driver, err := rrm.podLister.getDriverPod(ctx, appID, namespace)
	if err != nil {
		return 0, err
	}
	sparkResources, err := sparkResources(ctx, driver)
	if err != nil {
		return 0, err
	}
	maxAllowedExtraExecutors := sparkResources.MaxExecutorCount - sparkResources.MinExecutorCount
	return int(math.Max(float64(maxAllowedExtraExecutors-usedSoftReservationCount), 0)), nil
}

// getActivePods returns a map of pod names to pods that are still active in the passed pod's namespace
func (rrm *ResourceReservationManager) getActivePods(ctx context.Context, appID string, namespace string) (map[string]*v1.Pod, error) {
	selector := labels.Set(map[string]string{common.SparkAppIDLabel: appID}).AsSelector()
	pods, err := rrm.podLister.Pods(namespace).List(selector)
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

func (rrm *ResourceReservationManager) onExecutorPodDeletion(obj interface{}) {
	pod, ok := utils.GetPodFromObjectOrTombstone(obj)
	if !ok {
		svc1log.FromContext(rrm.context).Warn("failed to parse object as pod, skipping")
		return
	}

	// Only keep executor pods which are for dynamic allocation applications and which have resource reservations to give back
	if _, ok = rrm.softReservationStore.GetSoftReservation(pod.Labels[common.SparkAppIDLabel]); ok {
		if !rrm.softReservationStore.ExecutorHasSoftReservation(rrm.context, pod) {
			rrm.addPodForDynamicAllocationCompaction(pod)
		}
	}
}

func (rrm *ResourceReservationManager) addPodForDynamicAllocationCompaction(pod *v1.Pod) {
	rrm.dynamicAllocationCompactionSliceLock.Lock()
	defer rrm.dynamicAllocationCompactionSliceLock.Unlock()
	rrm.dynamicAllocationCompactionApps[pod.Labels[common.SparkAppIDLabel]] = pod.Namespace
}

// newResourceReservation builds a reservation object with the pods and resources passed and returns it.
func newResourceReservation(driverNode string, executorNodes []string, driver *v1.Pod, driverResources, executorResources *resources.Resources) *v1beta2.ResourceReservation {
	reservations := make(map[string]v1beta2.Reservation, len(executorNodes)+1)
	reservations["driver"] = v1beta2.Reservation{
		Node: driverNode,
		Resources: v1beta2.ResourceList{
			string(v1beta2.ResourceCPU):       &driverResources.CPU,
			string(v1beta2.ResourceMemory):    &driverResources.Memory,
			string(v1beta2.ResourceNvidiaGPU): &driverResources.NvidiaGPU,
		},
	}
	for idx, nodeName := range executorNodes {
		reservations[executorReservationName(idx)] = v1beta2.Reservation{
			Node: nodeName,
			Resources: v1beta2.ResourceList{
				string(v1beta2.ResourceCPU):       &executorResources.CPU,
				string(v1beta2.ResourceMemory):    &executorResources.Memory,
				string(v1beta2.ResourceNvidiaGPU): &executorResources.NvidiaGPU,
			},
		}
	}
	return &v1beta2.ResourceReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name:            driver.Labels[common.SparkAppIDLabel],
			Namespace:       driver.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(driver, podGroupVersionKind)},
			Labels: map[string]string{
				v1beta1.AppIDLabel: driver.Labels[common.SparkAppIDLabel],
			},
		},
		Spec: v1beta2.ResourceReservationSpec{
			Reservations: reservations,
		},
		Status: v1beta2.ResourceReservationStatus{
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
