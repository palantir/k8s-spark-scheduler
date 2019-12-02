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
	"time"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/logging"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/internal"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/k8s-spark-scheduler/internal/events"
	"github.com/palantir/k8s-spark-scheduler/internal/metrics"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/labels"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api"
)

const (
	failureUnbound                = "failure-unbound"
	failureInternal               = "failure-internal"
	failureFit                    = "failure-fit"
	failureEarlierDriver          = "failure-earlier-driver"
	failureNonSparkPod            = "failure-non-spark-pod"
	success                       = "success"
	successRescheduled            = "success-rescheduled"
	successAlreadyBound           = "success-already-bound"
	successScheduledExtraExecutor = "success-scheduled-extra-executor"
	failureFitExtraExecutor       = "failure-fit-extra-executor"
	// TODO: make this configurable
	// leaderElectionInterval is the default LeaseDuration for core clients.
	// obtained from k8s.io/component-base/config/v1alpha1
	leaderElectionInterval = 15 * time.Second
)

// SparkSchedulerExtender is a kubernetes scheduler extended responsible for ensuring
// a spark driver and all of the executors can be scheduled together given current
// resources available across the nodes
type SparkSchedulerExtender struct {
	nodeLister           corelisters.NodeLister
	podLister            *SparkPodLister
	resourceReservations *cache.ResourceReservationCache
	softReservationStore *cache.SoftReservationStore
	coreClient           corev1.CoreV1Interface

	demands             *cache.SafeDemandCache
	apiExtensionsClient apiextensionsclientset.Interface

	isFIFO                        bool
	binpacker                     *Binpacker
	overheadComputer              *OverheadComputer
	lastRequest                   time.Time
	instanceGroupLabel            string
	useExperimentalHostPriorities bool
}

// NewExtender is responsible for creating and initializing a SparkSchedulerExtender
func NewExtender(
	nodeLister corelisters.NodeLister,
	podLister *SparkPodLister,
	resourceReservations *cache.ResourceReservationCache,
	softReservationStore *cache.SoftReservationStore,
	coreClient corev1.CoreV1Interface,
	demands *cache.SafeDemandCache,
	apiExtensionsClient apiextensionsclientset.Interface,
	isFIFO bool,
	binpacker *Binpacker,
	overheadComputer *OverheadComputer,
	instanceGroupLabel string,
	useExperimentalHostPriorities bool) *SparkSchedulerExtender {
	return &SparkSchedulerExtender{
		nodeLister:                    nodeLister,
		podLister:                     podLister,
		resourceReservations:          resourceReservations,
		softReservationStore:          softReservationStore,
		coreClient:                    coreClient,
		demands:                       demands,
		apiExtensionsClient:           apiExtensionsClient,
		isFIFO:                        isFIFO,
		binpacker:                     binpacker,
		overheadComputer:              overheadComputer,
		instanceGroupLabel:            instanceGroupLabel,
		useExperimentalHostPriorities: useExperimentalHostPriorities,
	}
}

// Predicate is responsible for returning a filtered list of nodes that qualify to schedule the pod provided in the
// ExtenderArgs
func (s *SparkSchedulerExtender) Predicate(ctx context.Context, args schedulerapi.ExtenderArgs) *schedulerapi.ExtenderFilterResult {
	params := internal.PodSafeParams(args.Pod)
	role := args.Pod.Labels[SparkRoleLabel]
	instanceGroup := args.Pod.Spec.NodeSelector[s.instanceGroupLabel]
	params["podSparkRole"] = role
	params["instanceGroup"] = instanceGroup
	ctx = svc1log.WithLoggerParams(ctx, svc1log.SafeParams(params))
	logger := svc1log.FromContext(ctx)

	timer := metrics.NewScheduleTimer(ctx, instanceGroup, &args.Pod)
	logger.Info("starting scheduling pod")

	err := s.reconcileIfNeeded(ctx, timer)
	if err != nil {
		msg := "failed to reconcile"
		logger.Error(msg, svc1log.Stacktrace(err))
		return failWithMessage(ctx, args, msg)
	}

	nodeName, outcome, err := s.selectNode(ctx, args.Pod.Labels[SparkRoleLabel], &args.Pod, *args.NodeNames)
	timer.Mark(ctx, role, outcome)
	if err != nil {
		if outcome == failureInternal {
			logger.Error("internal error scheduling pod", svc1log.Stacktrace(err))
		} else {
			logger.Info("failed to schedule pod", svc1log.SafeParam("outcome", outcome), svc1log.SafeParam("reason", err.Error()))
		}
		return failWithMessage(ctx, args, err.Error())
	}

	if role == Driver {
		appResources, err := sparkResources(ctx, &args.Pod)
		if err != nil {
			logger.Error("internal error scheduling pod", svc1log.Stacktrace(err))
			return failWithMessage(ctx, args, err.Error())
		}
		events.EmitApplicationScheduled(
			ctx,
			instanceGroup,
			args.Pod.Labels[SparkAppIDLabel],
			args.Pod,
			appResources.driverResources,
			appResources.executorResources,
			appResources.minExecutorCount,
			appResources.maxExecutorCount)
	}

	logger.Info("scheduling pod to node", svc1log.SafeParam("nodeName", nodeName))
	return &schedulerapi.ExtenderFilterResult{NodeNames: &[]string{nodeName}}
}

func failWithMessage(ctx context.Context, args schedulerapi.ExtenderArgs, message string) *schedulerapi.ExtenderFilterResult {
	failedNodes := make(schedulerapi.FailedNodesMap, len(*args.NodeNames))
	for _, name := range *args.NodeNames {
		failedNodes[name] = message
	}
	return &schedulerapi.ExtenderFilterResult{FailedNodes: failedNodes}
}

func (s *SparkSchedulerExtender) reconcileIfNeeded(ctx context.Context, timer *metrics.ScheduleTimer) error {
	now := time.Now()
	if now.After(s.lastRequest.Add(leaderElectionInterval)) {
		err := s.syncResourceReservationsAndDemands(ctx)
		if err != nil {
			return err
		}
		timer.MarkReconciliationFinished(ctx)
	}
	s.lastRequest = now
	return nil
}

func (s *SparkSchedulerExtender) selectNode(ctx context.Context, role string, pod *v1.Pod, nodeNames []string) (string, string, error) {
	switch role {
	case Driver:
		return s.selectDriverNode(ctx, pod, nodeNames)
	case Executor:
		return s.selectExecutorNode(ctx, pod, nodeNames)
	default:
		return "", failureNonSparkPod, werror.Error("can not schedule non spark pod")
	}
}

// fitEarlierDrivers binpacks all given spark applications to the cluster and
// accounts for their resource usage in availableNodesSchedulingMetadata
func (s *SparkSchedulerExtender) fitEarlierDrivers(
	ctx context.Context,
	drivers []*v1.Pod,
	nodeNames, executorNodeNames []string,
	availableNodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) bool {
	for _, driver := range drivers {
		applicationResources, err := sparkResources(ctx, driver)
		if err != nil {
			svc1log.FromContext(ctx).Warn("failed to get driver resources, skipping driver",
				svc1log.SafeParam("faultyDriverName", driver.Name),
				svc1log.SafeParam("reason", err.Error))
			continue
		}
		driverNode, executorNodes, hasCapacity := s.binpacker.BinpackFunc(
			ctx,
			applicationResources.driverResources,
			applicationResources.executorResources,
			applicationResources.minExecutorCount,
			nodeNames, executorNodeNames, availableNodesSchedulingMetadata)
		if !hasCapacity {
			svc1log.FromContext(ctx).Warn("failed to fit one of the earlier drivers",
				svc1log.SafeParam("earlierDriverName", driver.Name))
			return false
		}
		availableNodesSchedulingMetadata.SubtractUsageIfExists(sparkResourceUsage(
			applicationResources.driverResources,
			applicationResources.executorResources,
			driverNode, executorNodes))
	}
	return true
}

func (s *SparkSchedulerExtender) selectDriverNode(ctx context.Context, driver *v1.Pod, nodeNames []string) (string, string, error) {
	if rr, ok := s.resourceReservations.Get(driver.Namespace, driver.Labels[SparkAppIDLabel]); ok {
		driverReservedNode := rr.Spec.Reservations["driver"].Node
		for _, node := range nodeNames {
			if driverReservedNode == node {
				svc1log.FromContext(ctx).Info("Received request to schedule driver which already has a reservation. Returning previously reserved node.",
					svc1log.SafeParam("driverReservedNode", driverReservedNode))
				return driverReservedNode, success, nil
			}
		}
		svc1log.FromContext(ctx).Warn("Received request to schedule driver which already has a reservation, but previously reserved node is not in list of nodes. Returning previously reserved node anyway.",
			svc1log.SafeParam("driverReservedNode", driverReservedNode),
			svc1log.SafeParam("nodeNames", nodeNames))
		return driverReservedNode, success, nil
	}
	availableNodes, err := s.nodeLister.List(labels.Set(driver.Spec.NodeSelector).AsSelector())
	if err != nil {
		return "", failureInternal, err
	}

	usages := s.usedResources()
	usages.Add(s.overheadComputer.GetOverhead(ctx, availableNodes))
	availableNodesSchedulingMetadata := resources.NodeSchedulingMetadataForNodes(availableNodes, usages)
	driverNodeNames, executorNodeNames := s.potentialNodes(availableNodesSchedulingMetadata, driver, nodeNames)
	applicationResources, err := sparkResources(ctx, driver)
	if err != nil {
		return "", failureInternal, werror.Wrap(err, "failed to get spark resources")
	}
	if s.isFIFO {
		queuedDrivers, err := s.podLister.ListEarlierDrivers(driver)
		if err != nil {
			return "", failureInternal, werror.Wrap(err, "failed to list earlier drivers")
		}
		ok := s.fitEarlierDrivers(ctx, queuedDrivers, driverNodeNames, executorNodeNames, availableNodesSchedulingMetadata)
		if !ok {
			s.createDemandForApplication(ctx, driver, applicationResources)
			return "", failureEarlierDriver, werror.Error("earlier drivers do not fit to the cluster")
		}
	}
	driverNode, executorNodes, hasCapacity := s.binpacker.BinpackFunc(
		ctx,
		applicationResources.driverResources,
		applicationResources.executorResources,
		applicationResources.minExecutorCount,
		driverNodeNames,
		executorNodeNames,
		availableNodesSchedulingMetadata)
	svc1log.FromContext(ctx).Debug("binpacking result",
		svc1log.SafeParam("availableNodesSchedulingMetadata", availableNodesSchedulingMetadata),
		svc1log.SafeParam("driverResources", applicationResources.driverResources),
		svc1log.SafeParam("executorResources", applicationResources.executorResources),
		svc1log.SafeParam("minExecutorCount", applicationResources.minExecutorCount),
		svc1log.SafeParam("maxExecutorCount", applicationResources.maxExecutorCount),
		svc1log.SafeParam("hasCapacity", hasCapacity),
		svc1log.SafeParam("candidateDriverNodes", nodeNames),
		svc1log.SafeParam("candidateExecutorNodes", executorNodeNames),
		svc1log.SafeParam("driverNode", driverNode),
		svc1log.SafeParam("executorNodes", executorNodes),
		svc1log.SafeParam("binpacker", s.binpacker.Name))
	if !hasCapacity {
		s.createDemandForApplication(ctx, driver, applicationResources)
		return "", failureFit, werror.Error("application does not fit to the cluster")
	}
	s.removeDemandIfExists(ctx, driver)
	metrics.ReportCrossZoneMetric(ctx, driverNode, executorNodes, availableNodes)
	reservedDriverNode, outcome, err := s.createResourceReservations(ctx, driver, applicationResources, driverNode, executorNodes)
	if outcome == success && applicationResources.maxExecutorCount > applicationResources.minExecutorCount {
		// only create soft reservations for applications which can request extra executors
		s.softReservationStore.CreateSoftReservationIfNotExists(driver.Labels[SparkAppIDLabel])
	}
	return reservedDriverNode, outcome, err
}

func (s *SparkSchedulerExtender) potentialNodes(availableNodesSchedulingMetadata resources.NodeGroupSchedulingMetadata, driver *v1.Pod, nodeNames []string) (driverNodes, executorNodes []string) {
	nodesInPriorityOrder := getNodeNamesInPriorityOrder(s.useExperimentalHostPriorities, availableNodesSchedulingMetadata)
	driverNodeNames := make([]string, 0, len(nodesInPriorityOrder))
	executorNodeNames := make([]string, 0, len(nodesInPriorityOrder))

	nodeNamesSet := make(map[string]interface{})
	for _, item := range nodeNames {
		nodeNamesSet[item] = nil
	}

	for _, nodeName := range nodesInPriorityOrder {
		if _, ok := nodeNamesSet[nodeName]; ok {
			driverNodeNames = append(driverNodeNames, nodeName)
		}
		if !availableNodesSchedulingMetadata[nodeName].Unschedulable && availableNodesSchedulingMetadata[nodeName].Ready {
			executorNodeNames = append(executorNodeNames, nodeName)
		}
	}
	return driverNodeNames, executorNodeNames
}

func (s *SparkSchedulerExtender) selectExecutorNode(ctx context.Context, executor *v1.Pod, nodeNames []string) (string, string, error) {
	resourceReservation, ok := s.resourceReservations.Get(executor.Namespace, executor.Labels[SparkAppIDLabel])
	if !ok {
		return "", failureInternal, werror.Error("failed to get resource reservations")
	}
	unboundReservations, outcome, unboundResErr := s.findUnboundReservations(ctx, executor, resourceReservation)
	if unboundResErr != nil {
		extraExecutorCount := 0
		if sr, ok := s.softReservationStore.GetSoftReservation(executor.Labels[SparkAppIDLabel]); ok {
			extraExecutorCount = len(sr.Reservations)
		}
		driver, err := s.podLister.getDriverPod(ctx, executor)
		if err != nil {
			return "", failureInternal, err
		}
		sparkResources, err := sparkResources(ctx, driver)
		if err != nil {
			return "", failureInternal, err
		}
		if outcome == failureUnbound && (sparkResources.minExecutorCount+extraExecutorCount) < sparkResources.maxExecutorCount {
			// dynamic allocation case where driver is requesting more executors than min but less than max
			node, outcome, err := s.rescheduleExecutor(ctx, executor, nodeNames, sparkResources, false)
			if err != nil {
				if outcome == failureFit {
					return node, failureFitExtraExecutor, werror.Error("not enough capacity to schedule the extra executor")
				}
				return node, outcome, err
			}
			softReservation := v1beta1.Reservation{
				Node:   node,
				CPU:    sparkResources.executorResources.CPU,
				Memory: sparkResources.executorResources.Memory,
			}
			err = s.softReservationStore.AddReservationForPod(ctx, driver.Labels[SparkAppIDLabel], executor.Name, softReservation)
			if err != nil {
				return "", failureInternal, err
			}
			// We might have created a demand object for this executor when we were under min count, so we should remove if it exists
			s.removeDemandIfExists(ctx, executor)
			return node, successScheduledExtraExecutor, nil
		}
		return "", outcome, unboundResErr
	}
	// the reservation to be selected for the current executor needs to be on a node that the extender has received from kube-scheduler
	nodeToReservation := make(map[string]string, len(unboundReservations))
	for _, reservationName := range unboundReservations {
		nodeToReservation[resourceReservation.Spec.Reservations[reservationName].Node] = reservationName
	}
	var unboundReservation string
	for _, name := range nodeNames {
		if reservation, ok := nodeToReservation[name]; ok {
			unboundReservation = reservation
			break
		}
	}
	copyResourceReservation := resourceReservation.DeepCopy()

	if unboundReservation == "" {
		// no nodes for the unbound reservations exists in nodeNames, this might be because the node for the reservations are terminated
		// try to reschedule the executor, breaking FIFO, but preventing executor starvation
		// we are guaranteed len(unboundResourceReservations) > 0
		unboundReservation = unboundReservations[0]
		driver, err := s.podLister.getDriverPod(ctx, executor)
		if err != nil {
			return "", failureInternal, err
		}
		sparkResources, err := sparkResources(ctx, driver)
		if err != nil {
			return "", failureInternal, err
		}
		node, outcome, err := s.rescheduleExecutor(ctx, executor, nodeNames, sparkResources, true)
		if err != nil {
			return "", outcome, err
		}
		svc1log.FromContext(ctx).Info("rescheduling executor", svc1log.SafeParam("reservationName", unboundReservation), svc1log.SafeParam("node", node))
		reservation := copyResourceReservation.Spec.Reservations[unboundReservation]
		reservation.Node = node
		copyResourceReservation.Spec.Reservations[unboundReservation] = reservation
	}

	copyResourceReservation.Status.Pods[unboundReservation] = executor.Name
	err := s.resourceReservations.Update(copyResourceReservation)
	if err != nil {
		return "", failureInternal, werror.Wrap(err, "failed to update resource reservation")
	}
	s.removeDemandIfExists(ctx, executor)
	return copyResourceReservation.Spec.Reservations[unboundReservation].Node, outcome, err
}

func (s *SparkSchedulerExtender) getNodes(ctx context.Context, nodeNames []string) []*v1.Node {
	availableNodes := make([]*v1.Node, 0, len(nodeNames))
	for _, name := range nodeNames {
		node, err := s.nodeLister.Get(name)
		if err != nil {
			svc1log.FromContext(ctx).Warn("failed to find node in cache, skipping node",
				svc1log.SafeParam("nodeName", name))
			continue
		}
		availableNodes = append(availableNodes, node)
	}
	return availableNodes
}

func (s *SparkSchedulerExtender) usedResources() resources.NodeGroupResources {
	resourceReservations := s.resourceReservations.List()
	usage := resources.UsageForNodes(resourceReservations)
	usage.Add(s.softReservationStore.UsedSoftReservationResources())
	return usage
}

func (s *SparkSchedulerExtender) createResourceReservations(
	ctx context.Context,
	driver *v1.Pod,
	applicationResources *sparkApplicationResources,
	driverNode string,
	executorNodes []string) (string, string, error) {
	logger := svc1log.FromContext(ctx)
	rr := newResourceReservation(driverNode, executorNodes, driver, applicationResources.driverResources, applicationResources.executorResources)
	err := s.resourceReservations.Create(rr)
	if err != nil {
		return "", failureInternal, werror.Wrap(err, "failed to create resource reservation", werror.SafeParam("reservationName", rr.Name))
	}
	logger.Debug("creating executor resource reservations", svc1log.SafeParams(logging.RRSafeParam(rr)))
	return driverNode, success, nil
}

func (s *SparkSchedulerExtender) rescheduleExecutor(ctx context.Context, executor *v1.Pod, nodeNames []string, applicationResources *sparkApplicationResources, createDemandIfNoFit bool) (string, string, error) {
	executorResources := &resources.Resources{CPU: applicationResources.executorResources.CPU, Memory: applicationResources.executorResources.Memory}
	availableNodes := s.getNodes(ctx, nodeNames)
	usages := s.usedResources()
	usages.Add(s.overheadComputer.GetOverhead(ctx, availableNodes))
	availableResources := resources.AvailableForNodes(availableNodes, usages)
	for _, name := range nodeNames {
		if !executorResources.GreaterThan(availableResources[name]) {
			return name, successRescheduled, nil
		}
	}

	if createDemandIfNoFit {
		s.createDemandForExecutor(ctx, executor, executorResources)
	}
	return "", failureFit, werror.Error("not enough capacity to reschedule the executor")
}

// if err is nil, it is guaranteed to return a non empty array of resource reservations
func (s *SparkSchedulerExtender) findUnboundReservations(ctx context.Context, executor *v1.Pod, resourceReservation *v1beta1.ResourceReservation) ([]string, string, error) {
	unboundReservations := make([]string, 0, len(resourceReservation.Spec.Reservations))
	// first try to find an unbound executor reservation
	for name := range resourceReservation.Spec.Reservations {
		podName, ok := resourceReservation.Status.Pods[name]
		if !ok {
			unboundReservations = append(unboundReservations, name)
		}
		if podName == executor.Name {
			// binding reservations have to be idempotent. Binding the pod to the node on kube-scheduler might fail, so we can get the same executor pod as a retry.
			svc1log.FromContext(ctx).Info("found already bound resource reservation for the current pod", svc1log.SafeParam("reservationName", name))
			return []string{name}, successAlreadyBound, nil
		}
	}
	if len(unboundReservations) > 0 {
		return unboundReservations, success, nil
	}
	// No unbound reservations exist, so iterate over existing reservations to see if any of the reserved pods are dead.
	// Spark will recreate lost executors, so the replacement executors should be placed on the reserved spaces of dead executors.
	selector := labels.Set(map[string]string{SparkAppIDLabel: executor.Labels[SparkAppIDLabel]}).AsSelector()
	pods, err := s.podLister.Pods(executor.Namespace).List(selector)
	if err != nil {
		return nil, failureInternal, werror.Wrap(err, "failed to list pods")
	}
	activePodNames := make(map[string]bool, len(pods))
	for _, pod := range pods {
		if !isPodTerminated(pod) {
			activePodNames[pod.Name] = true
		}
	}
	relocatableReservations := make([]string, 0, len(resourceReservation.Spec.Reservations))
	for name := range resourceReservation.Spec.Reservations {
		podIdentifier := resourceReservation.Status.Pods[name]
		if !activePodNames[podIdentifier] {
			relocatableReservations = append(relocatableReservations, name)
		}
	}
	if len(relocatableReservations) == 0 {
		return nil, failureUnbound, werror.Error("failed to find unbound resource reservation", werror.SafeParams(logging.RRSafeParam(resourceReservation)))
	}
	svc1log.FromContext(ctx).Info("found relocatable resource reservations", svc1log.SafeParams(logging.RRSafeParam(resourceReservation)))
	return relocatableReservations, successRescheduled, nil
}

func isPodTerminated(pod *v1.Pod) bool {
	allTerminated := len(pod.Status.ContainerStatuses) > 0
	for _, status := range pod.Status.ContainerStatuses {
		allTerminated = allTerminated && status.State.Terminated != nil
	}
	return allTerminated
}
