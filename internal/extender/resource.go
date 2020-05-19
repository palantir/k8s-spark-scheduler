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

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/internal"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	"github.com/palantir/k8s-spark-scheduler/internal/common/utils"
	"github.com/palantir/k8s-spark-scheduler/internal/events"
	"github.com/palantir/k8s-spark-scheduler/internal/metrics"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/predicates"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/extender/v1"
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
	nodeLister                 corelisters.NodeLister
	podLister                  *SparkPodLister
	resourceReservations       *cache.ResourceReservationCache
	softReservationStore       *cache.SoftReservationStore
	resourceReservationManager *ResourceReservationManager
	coreClient                 corev1.CoreV1Interface
	nodeSorter                 *NodeSorter

	demands             *cache.SafeDemandCache
	apiExtensionsClient apiextensionsclientset.Interface

	isFIFO                        bool
	binpacker                     *Binpacker
	overheadComputer              *OverheadComputer
	lastRequest                   time.Time
	instanceGroupLabel            string
}

// NewExtender is responsible for creating and initializing a SparkSchedulerExtender
func NewExtender(
	nodeLister corelisters.NodeLister,
	podLister *SparkPodLister,
	resourceReservations *cache.ResourceReservationCache,
	softReservationStore *cache.SoftReservationStore,
	resourceReservationManager *ResourceReservationManager,
	coreClient corev1.CoreV1Interface,
	demands *cache.SafeDemandCache,
	apiExtensionsClient apiextensionsclientset.Interface,
	isFIFO bool,
	binpacker *Binpacker,
	overheadComputer *OverheadComputer,
	instanceGroupLabel string,
	nodeSorter *NodeSorter) *SparkSchedulerExtender {
	return &SparkSchedulerExtender{
		nodeLister:                           nodeLister,
		podLister:                            podLister,
		resourceReservations:                 resourceReservations,
		softReservationStore:                 softReservationStore,
		resourceReservationManager:           resourceReservationManager,
		coreClient:                           coreClient,
		demands:                              demands,
		apiExtensionsClient:                  apiExtensionsClient,
		isFIFO:                               isFIFO,
		binpacker:                            binpacker,
		overheadComputer:                     overheadComputer,
		instanceGroupLabel:                   instanceGroupLabel,
		nodeSorter: nodeSorter,
	}
}

// Predicate is responsible for returning a filtered list of nodes that qualify to schedule the pod provided in the
// ExtenderArgs
func (s *SparkSchedulerExtender) Predicate(ctx context.Context, args schedulerapi.ExtenderArgs) *schedulerapi.ExtenderFilterResult {
	params := internal.PodSafeParams(*args.Pod)
	role := args.Pod.Labels[common.SparkRoleLabel]
	ctx = svc1log.WithLoggerParams(ctx, svc1log.SafeParams(params))
	logger := svc1log.FromContext(ctx)
	instanceGroup, success := internal.FindInstanceGroupFromPodSpec(args.Pod.Spec, s.instanceGroupLabel)
	if !success {
		instanceGroup = ""
	}
	params["podSparkRole"] = role
	params["instanceGroup"] = instanceGroup

	timer := metrics.NewScheduleTimer(ctx, instanceGroup, args.Pod)
	logger.Info("starting scheduling pod")

	err := s.reconcileIfNeeded(ctx, timer)
	if err != nil {
		msg := "failed to reconcile"
		logger.Error(msg, svc1log.Stacktrace(err))
		return failWithMessage(ctx, args, msg)
	}

	nodeName, outcome, err := s.selectNode(ctx, args.Pod.Labels[common.SparkRoleLabel], args.Pod, *args.NodeNames)
	timer.Mark(ctx, role, outcome)
	if err != nil {
		if outcome == failureInternal {
			logger.Error("internal error scheduling pod", svc1log.Stacktrace(err))
		} else {
			logger.Info("failed to schedule pod", svc1log.SafeParam("outcome", outcome), svc1log.SafeParam("reason", err.Error()))
		}
		return failWithMessage(ctx, args, err.Error())
	}

	if role == common.Driver {
		appResources, err := sparkResources(ctx, args.Pod)
		if err != nil {
			logger.Error("internal error scheduling pod", svc1log.Stacktrace(err))
			return failWithMessage(ctx, args, err.Error())
		}
		events.EmitApplicationScheduled(
			ctx,
			instanceGroup,
			args.Pod.Labels[common.SparkAppIDLabel],
			*args.Pod,
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
	case common.Driver:
		return s.selectDriverNode(ctx, pod, nodeNames)
	case common.Executor:
		node, outcome, err := s.selectExecutorNode(ctx, pod, nodeNames)
		if s.isSuccessOutcome(outcome) {
			s.removeDemandIfExists(ctx, pod)
		}
		return node, outcome, err
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
	if rr, ok := s.resourceReservationManager.GetResourceReservation(driver); ok {
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
	availableNodes, err := s.nodeLister.ListWithPredicate(func(node *v1.Node) bool {
		return predicates.PodMatchesNodeSelectorAndAffinityTerms(driver, node)
	})
	if err != nil {
		return "", failureInternal, err
	}

	usages := s.resourceReservationManager.GetReservedResources()
	usages.Add(s.overheadComputer.GetOverhead(ctx, availableNodes))
	availableNodesSchedulingMetadata := resources.NodeSchedulingMetadataForNodes(availableNodes, usages)
	driverNodeNames, executorNodeNames := s.nodeSorter.potentialNodes(availableNodesSchedulingMetadata, nodeNames)
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

	_, err = s.resourceReservationManager.CreateReservations(ctx, driver, applicationResources, driverNode, executorNodes)
	if err != nil {
		return "", failureInternal, err
	}
	return driverNode, success, nil
}

func (s *SparkSchedulerExtender) selectExecutorNode(ctx context.Context, executor *v1.Pod, nodeNames []string) (string, string, error) {
	alreadyBoundNode, found, err := s.resourceReservationManager.FindAlreadyBoundReservationNode(ctx, executor)
	if err != nil {
		return "", failureInternal, werror.WrapWithContextParams(ctx, err, "error when looking for already bound reservations")
	}
	if found {
		// check that it is part of the nodes passed and return it
		if resultNode, ok := s.getReservationNodeFromNodeList([]string{alreadyBoundNode}, nodeNames); ok {
			svc1log.FromContext(ctx).Info("found already bound reservation node for executor", svc1log.SafeParam("nodeName", resultNode))
			return resultNode, successAlreadyBound, nil
		}
		svc1log.FromContext(ctx).Info("found already bound reservation node for executor, but it was not part of the potential nodes", svc1log.SafeParam("nodeName", alreadyBoundNode))
	}

	unboundReservationNodes, foundUnbound, err := s.resourceReservationManager.FindUnboundReservationNodes(ctx, executor)
	if err != nil {
		return "", failureInternal, werror.WrapWithContextParams(ctx, err, "error when looking for unbound reservations")
	}
	if foundUnbound {
		if resultNode, ok := s.getReservationNodeFromNodeList(unboundReservationNodes, nodeNames); ok {
			svc1log.FromContext(ctx).Info("found unbound reservation node for executor", svc1log.SafeParam("nodeName", resultNode))
			err := s.resourceReservationManager.ReserveForExecutorOnUnboundReservation(ctx, executor, resultNode)
			if err != nil {
				return "", failureInternal, werror.WrapWithContextParams(ctx, err, "failed to reserve node for executor")
			}
			return resultNode, success, nil
		}
		svc1log.FromContext(ctx).Info("found unbound reservation nodes for executor, but none were part of the potential nodes", svc1log.SafeParam("nodeNames", unboundReservationNodes))
	}

	// Else, check if you still can have an executor, and if yes, reschedule
	freeExecutorSpots, err := s.resourceReservationManager.GetRemainingAllowedExecutorCount(ctx, executor)
	if err != nil {
		return "", failureInternal, werror.WrapWithContextParams(ctx, err, "error when checking for remaining allowed executor count")
	}
	if freeExecutorSpots > 0 {
		// We can assume it's a dynamic allocation executor if we can still have executors even though all the reservations are bound
		// (might change by the time we reserve, but that's alright)
		isExtraExecutor := !foundUnbound
		nodeName, outcome, err := s.rescheduleExecutor(ctx, executor, nodeNames, isExtraExecutor)
		if err != nil {
			return "", outcome, werror.WrapWithContextParams(ctx, err, "failed to reschedule executor")
		}
		svc1log.FromContext(ctx).Info("rescheduling executor onto node", svc1log.SafeParam("nodeName", nodeName), svc1log.SafeParam("isExtraExecutor", isExtraExecutor))
		err = s.resourceReservationManager.ReserveForExecutorOnRescheduledNode(ctx, executor, nodeName)
		if err != nil {
			return "", failureInternal, werror.WrapWithContextParams(ctx, err, "failed to reserve node for rescheduled executor")
		}
		return nodeName, outcome, nil
	}

	return "", failureUnbound, werror.ErrorWithContextParams(ctx, "application has no free executor spots to schedule this one")
}

// getReservationNodeFromNodeList filters the list of reservationNodes to return a single one that also appears in nodeNames, or false.
func (s *SparkSchedulerExtender) getReservationNodeFromNodeList(reservationNodes []string, nodeNames []string) (string, bool) {
	reservationNodeSet := utils.NewStringSet(len(reservationNodes))
	reservationNodeSet.AddAll(reservationNodes)
	for _, name := range nodeNames {
		if reservationNodeSet.Contains(name) {
			return name, true
		}
	}
	return "", false
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

func (s *SparkSchedulerExtender) rescheduleExecutor(ctx context.Context, executor *v1.Pod, nodeNames []string, isExtraExecutor bool) (string, string, error) {
	driver, err := s.podLister.getDriverPod(ctx, executor)
	if err != nil {
		return "", failureInternal, err
	}
	sparkResources, err := sparkResources(ctx, driver)
	if err != nil {
		return "", failureInternal, err
	}
	executorResources := &resources.Resources{CPU: sparkResources.executorResources.CPU, Memory: sparkResources.executorResources.Memory}
	availableNodes := s.getNodes(ctx, nodeNames)
	usages := s.resourceReservationManager.GetReservedResources()
	usages.Add(s.overheadComputer.GetOverhead(ctx, availableNodes))
	availableResources := resources.AvailableForNodes(availableNodes, usages)
	for _, name := range nodeNames {
		if !executorResources.GreaterThan(availableResources[name]) {
			if isExtraExecutor {
				return name, successScheduledExtraExecutor, nil
			}
			return name, successRescheduled, nil
		}
	}

	if isExtraExecutor {
		return "", failureFitExtraExecutor, werror.ErrorWithContextParams(ctx, "not enough capacity to schedule the extra executor")
	}

	s.createDemandForExecutor(ctx, executor, executorResources)
	return "", failureFit, werror.ErrorWithContextParams(ctx, "not enough capacity to reschedule the executor")
}

func (s *SparkSchedulerExtender) isSuccessOutcome(outcome string) bool {
	return outcome == success || outcome == successAlreadyBound || outcome == successRescheduled || outcome == successScheduledExtraExecutor
}
