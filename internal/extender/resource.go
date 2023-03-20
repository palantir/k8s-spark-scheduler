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

	demandapi "github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha2"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/binpack"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/k8s-spark-scheduler/internal"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	"github.com/palantir/k8s-spark-scheduler/internal/common/utils"
	"github.com/palantir/k8s-spark-scheduler/internal/events"
	"github.com/palantir/k8s-spark-scheduler/internal/metrics"
	"github.com/palantir/k8s-spark-scheduler/internal/sort"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/labels"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	v1affinityhelper "k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	schedulerapi "k8s.io/kube-scheduler/extender/v1"
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
	nodeSorter                 *sort.NodeSorter

	demands             *cache.SafeDemandCache
	apiExtensionsClient apiextensionsclientset.Interface

	isFIFO                                              bool
	fifoConfig                                          config.FifoConfig
	binpacker                                           *Binpacker
	shouldScheduleDynamicallyAllocatedExecutorsInSameAZ bool
	overheadComputer                                    *OverheadComputer
	lastRequest                                         time.Time
	instanceGroupLabel                                  string

	wasteMetricsReporter *metrics.WasteMetricsReporter
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
	fifoConfig config.FifoConfig,
	binpacker *Binpacker,
	shouldScheduleDynamicallyAllocatedExecutorsInSameAZ bool,
	overheadComputer *OverheadComputer,
	instanceGroupLabel string,
	nodeSorter *sort.NodeSorter,
	wasteMetricsReporter *metrics.WasteMetricsReporter) *SparkSchedulerExtender {
	return &SparkSchedulerExtender{
		nodeLister:                 nodeLister,
		podLister:                  podLister,
		resourceReservations:       resourceReservations,
		softReservationStore:       softReservationStore,
		resourceReservationManager: resourceReservationManager,
		coreClient:                 coreClient,
		demands:                    demands,
		apiExtensionsClient:        apiExtensionsClient,
		isFIFO:                     isFIFO,
		fifoConfig:                 fifoConfig,
		binpacker:                  binpacker,
		shouldScheduleDynamicallyAllocatedExecutorsInSameAZ: shouldScheduleDynamicallyAllocatedExecutorsInSameAZ,
		overheadComputer:     overheadComputer,
		instanceGroupLabel:   instanceGroupLabel,
		nodeSorter:           nodeSorter,
		wasteMetricsReporter: wasteMetricsReporter,
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
	sparkAppID := args.Pod.Labels[common.SparkAppIDLabel]
	params["podSparkRole"] = role
	params["instanceGroup"] = instanceGroup
	params["sparkAppID"] = sparkAppID

	timer := metrics.NewScheduleTimer(ctx, instanceGroup, args.Pod)
	logger.Info("starting scheduling pod")

	err := s.reconcileIfNeeded(ctx, timer)
	if err != nil {
		msg := "failed to reconcile"
		logger.Error(msg, svc1log.Stacktrace(err))
		return s.failWithMessage(ctx, failureInternal, args, msg)
	}
	s.resourceReservationManager.CompactDynamicAllocationApplications(ctx)

	nodeName, outcome, err := s.selectNode(ctx, args.Pod.Labels[common.SparkRoleLabel], args.Pod, *args.NodeNames)
	timer.Mark(ctx, role, outcome)
	if err != nil {
		if outcome == failureInternal {
			logger.Error("internal error scheduling pod", svc1log.Stacktrace(err))
		} else {
			logger.Info("failed to schedule pod", svc1log.SafeParam("outcome", outcome), svc1log.SafeParam("reason", err.Error()))
		}
		return s.failWithMessage(ctx, outcome, args, err.Error())
	}

	if role == common.Driver {
		appResources, err := sparkResources(ctx, args.Pod)
		if err != nil {
			logger.Error("internal error scheduling pod", svc1log.Stacktrace(err))
			return s.failWithMessage(ctx, failureInternal, args, err.Error())
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

func (s *SparkSchedulerExtender) failWithMessage(ctx context.Context, outcome string, args schedulerapi.ExtenderArgs, message string) *schedulerapi.ExtenderFilterResult {
	s.wasteMetricsReporter.MarkFailedSchedulingAttempt(args.Pod, outcome)
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
		packingResult := s.binpacker.BinpackFunc(
			ctx,
			applicationResources.driverResources,
			applicationResources.executorResources,
			applicationResources.minExecutorCount,
			nodeNames, executorNodeNames, availableNodesSchedulingMetadata)
		if !packingResult.HasCapacity {
			if s.shouldSkipDriverFifo(driver) {
				svc1log.FromContext(ctx).Debug("Skipping non-fitting driver from FIFO consideration because it is not too old yet",
					svc1log.SafeParam("earlierDriverName", driver.Name))
				continue
			}
			svc1log.FromContext(ctx).Warn("failed to fit one of the earlier drivers",
				svc1log.SafeParam("earlierDriverName", driver.Name))
			return false
		}

		availableNodesSchedulingMetadata.SubtractUsageIfExists(sparkResourceUsage(
			applicationResources.driverResources,
			applicationResources.executorResources,
			packingResult.DriverNode,
			packingResult.ExecutorNodes))
	}
	return true
}

func (s *SparkSchedulerExtender) shouldSkipDriverFifo(pod *v1.Pod) bool {
	instanceGroup, success := internal.FindInstanceGroupFromPodSpec(pod.Spec, s.instanceGroupLabel)
	if !success {
		instanceGroup = ""
	}
	enforceAfterPodAgeConfig := s.fifoConfig.DefaultEnforceAfterPodAge
	if instanceGroupCustomConfig, ok := s.fifoConfig.EnforceAfterPodAgeByInstanceGroup[instanceGroup]; ok {
		enforceAfterPodAgeConfig = instanceGroupCustomConfig
	}
	return pod.CreationTimestamp.Add(enforceAfterPodAgeConfig).After(time.Now())
}

func (s *SparkSchedulerExtender) selectDriverNode(ctx context.Context, driver *v1.Pod, nodeNames []string) (string, string, error) {
	if rr, ok := s.resourceReservationManager.GetResourceReservation(driver.Labels[common.SparkAppIDLabel], driver.Namespace); ok {
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
	availableNodes, err := utils.ListWithPredicate(s.nodeLister, func(node *v1.Node) (bool, error) {
		match, error := v1affinityhelper.GetRequiredNodeAffinity(driver).Match(node)
		return match, error
	})
	if err != nil {
		return "", failureInternal, err
	}

	usage := s.resourceReservationManager.GetReservedResources()
	overhead := s.overheadComputer.GetOverhead(ctx, availableNodes)

	availableNodesSchedulingMetadata := resources.NodeSchedulingMetadataForNodes(availableNodes, usage, overhead)
	driverNodeNames, executorNodeNames := s.nodeSorter.PotentialNodes(availableNodesSchedulingMetadata, nodeNames)
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
			s.createDemandForApplicationInAnyZone(ctx, driver, applicationResources)
			return "", failureEarlierDriver, werror.Error("earlier drivers do not fit to the cluster")
		}
	}

	packingResult := s.binpacker.BinpackFunc(
		ctx,
		applicationResources.driverResources,
		applicationResources.executorResources,
		applicationResources.minExecutorCount,
		driverNodeNames,
		executorNodeNames,
		availableNodesSchedulingMetadata)
	efficiency := computeAvgPackingEfficiencyForResult(availableNodesSchedulingMetadata, packingResult)

	svc1log.FromContext(ctx).Debug("binpacking result",
		svc1log.SafeParam("availableNodesSchedulingMetadata", availableNodesSchedulingMetadata),
		svc1log.SafeParam("driverResources", applicationResources.driverResources),
		svc1log.SafeParam("executorResources", applicationResources.executorResources),
		svc1log.SafeParam("minExecutorCount", applicationResources.minExecutorCount),
		svc1log.SafeParam("maxExecutorCount", applicationResources.maxExecutorCount),
		svc1log.SafeParam("hasCapacity", packingResult.HasCapacity),
		svc1log.SafeParam("candidateDriverNodes", nodeNames),
		svc1log.SafeParam("candidateExecutorNodes", executorNodeNames),
		svc1log.SafeParam("driverNode", packingResult.DriverNode),
		svc1log.SafeParam("executorNodes", packingResult.ExecutorNodes),
		svc1log.SafeParam("avg packing efficiency CPU", efficiency.CPU),
		svc1log.SafeParam("avg packing efficiency Memory", efficiency.Memory),
		svc1log.SafeParam("avg packing efficiency GPU", efficiency.GPU),
		svc1log.SafeParam("avg packing efficiency Max", efficiency.Max),
		svc1log.SafeParam("binpacker", s.binpacker.Name))
	if !packingResult.HasCapacity {
		s.createDemandForApplicationInAnyZone(ctx, driver, applicationResources)
		return "", failureFit, werror.Error("application does not fit to the cluster")
	}

	metrics.ReportPackingEfficiency(ctx, s.binpacker.Name, efficiency)

	s.removeDemandIfExists(ctx, driver)
	metrics.ReportCrossZoneMetric(ctx, packingResult.DriverNode, packingResult.ExecutorNodes, availableNodes)

	_, err = s.resourceReservationManager.CreateReservations(
		ctx,
		driver,
		applicationResources,
		packingResult.DriverNode,
		packingResult.ExecutorNodes,
	)
	if err != nil {
		return "", failureInternal, err
	}
	return packingResult.DriverNode, success, nil
}

func computeAvgPackingEfficiencyForResult(
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	packingResult *binpack.PackingResult) binpack.AvgPackingEfficiency {

	packingEfficienciesDefault := make([]*binpack.PackingEfficiency, 0)
	for _, packingEfficiency := range packingResult.PackingEfficiencies {
		packingEfficienciesDefault = append(packingEfficienciesDefault, packingEfficiency)
	}
	return binpack.ComputeAvgPackingEfficiency(nodesSchedulingMetadata, packingEfficienciesDefault)
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
	freeExecutorSpots, err := s.resourceReservationManager.GetRemainingAllowedExecutorCount(ctx, executor.Labels[common.SparkAppIDLabel], executor.Namespace)
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

func filterNodesToZone(ctx context.Context, initialNodes []*v1.Node, zone string) ([]*v1.Node, error) {
	nodes := make([]*v1.Node, 0, len(initialNodes))
	for _, node := range initialNodes {
		zoneLabel, ok := node.Labels[v1.LabelTopologyZone]
		if !ok {
			return nil, werror.ErrorWithContextParams(ctx, "Could not read zone label from node, unable to make scheduling decisions based on AZ")
		}

		if zoneLabel == zone {
			nodes = append(nodes, node)
		}
	}
	nodeNames := getNodeNames(nodes)
	svc1log.FromContext(ctx).Info("Filtered nodes in zone", svc1log.SafeParam("zone", zone), svc1log.SafeParam("nodes", nodeNames))
	return nodes, nil
}

func getNodeNames(nodes []*v1.Node) []string {
	nodeNames := make([]string, len(nodes))
	for _, node := range nodes {
		nodeNames = append(nodeNames, node.Name)
	}
	return nodeNames
}

// Return tuple contains the following elements:
// ( String of the AZ in which the application is running if the application is running in a single AZ,
//
//	Bool: True if the application is running in a single AZ, false otherwise
//	Error: Non nil if we were unable to determine if the application was running in a single AZ )
func (s *SparkSchedulerExtender) getCommonZoneForExecutorsApplication(ctx context.Context, executor *v1.Pod) (string, bool, error) {
	executorSparkLabel, ok := executor.Labels[common.SparkAppIDLabel]
	if !ok {
		return "", false, werror.ErrorWithContextParams(ctx, "Executor does not have a Spark app id label, could not create label selector")
	}
	applicationPods, err := s.getSparkApplicationPodsForExecutor(ctx, executor, executorSparkLabel)
	if err != nil {
		return "", false, err
	}
	scheduledPods := filterToRunningPods(ctx, applicationPods)
	azs, err := getAzsOfPods(ctx, scheduledPods, s)
	if err != nil {
		return "", false, err
	}
	svc1log.FromContext(ctx).Info("Pods are scheduled in zones", svc1log.SafeParam("zones", azs.ToSlice()))
	if azs.Size() > 1 {
		return "", false, nil
	}
	if azs.Size() == 0 {
		return "", false, werror.ErrorWithContextParams(ctx, "Application has no scheduled pods, can't make scheduling decisions based on AZ")
	}
	return azs.ToSlice()[0], true, nil
}

func getAzsOfPods(ctx context.Context, runningPods []*v1.Pod, s *SparkSchedulerExtender) (utils.StringSet, error) {
	azs := utils.NewStringSet(len(runningPods))
	// Get all node AZs where pods are running
	for _, pod := range runningPods {
		nodeName := pod.Spec.NodeName
		node, err := s.nodeLister.Get(nodeName)
		if err != nil {
			return nil, err
		}
		zoneLabel, ok := node.Labels[v1.LabelTopologyZone]
		if !ok {
			return nil, werror.ErrorWithContextParams(ctx, "Could not read zone label from node, unable to make scheduling decisions based on AZ")
		}
		azs.Add(zoneLabel)
	}
	return azs, nil
}

// Filtering to running pods as pods in other states are not assigned to a node
func filterToRunningPods(ctx context.Context, applicationPods []*v1.Pod) []*v1.Pod {
	scheduledPods := make([]*v1.Pod, 0, len(applicationPods))
	for _, pod := range applicationPods {
		if pod.Status.Phase == v1.PodRunning {
			scheduledPods = append(scheduledPods, pod)
		}
	}
	logRunningPods(ctx, scheduledPods)
	return scheduledPods
}

func logRunningPods(ctx context.Context, scheduledPods []*v1.Pod) {
	scheduledPodNames := make([]string, 0, len(scheduledPods))
	for _, pod := range scheduledPods {
		scheduledPodNames = append(scheduledPodNames, pod.Name)
	}
	svc1log.FromContext(ctx).Info("Filtered to running application pods", svc1log.SafeParam("scheduledApplicationPods", scheduledPodNames))
}

func (s *SparkSchedulerExtender) getSparkApplicationPodsForExecutor(ctx context.Context, executor *v1.Pod, executorSparkLabel string) ([]*v1.Pod, error) {
	applicationPods, err := s.podLister.Pods(executor.Namespace).List(labels.Set(map[string]string{common.SparkAppIDLabel: executorSparkLabel}).AsSelector())
	if err != nil {
		return nil, err
	}
	logApplicationPods(ctx, applicationPods)
	return applicationPods, nil
}

func logApplicationPods(ctx context.Context, applicationPods []*v1.Pod) {
	applicationPodNames := make([]string, 0, len(applicationPods))
	for _, pod := range applicationPods {
		applicationPodNames = append(applicationPodNames, pod.Name)
	}
	svc1log.FromContext(ctx).Info("Found existing application pods", svc1log.SafeParam("applicationPodNames", applicationPodNames))
}

func (s *SparkSchedulerExtender) rescheduleExecutor(ctx context.Context, executor *v1.Pod, nodeNames []string, isExtraExecutor bool) (string, string, error) {
	initialNodeNames := nodeNames
	driver, err := s.podLister.getDriverPodForExecutor(ctx, executor)
	if err != nil {
		return "", failureInternal, err
	}
	sparkResources, err := sparkResources(ctx, driver)
	if err != nil {
		return "", failureInternal, err
	}
	executorResources := &resources.Resources{CPU: sparkResources.executorResources.CPU, Memory: sparkResources.executorResources.Memory, NvidiaGPU: sparkResources.executorResources.NvidiaGPU}
	availableNodes := s.getNodes(ctx, nodeNames)

	shouldScheduleIntoSingleAZ := false
	singleAzZone := ""
	if s.binpacker.IsSingleAz && s.shouldScheduleDynamicallyAllocatedExecutorsInSameAZ {
		svc1log.FromContext(ctx).Info("Dynamic Allocation single AZ scheduling enabled, attempting to get zone to schedule into.")
		zone, allPodsInSameAz, err := s.getCommonZoneForExecutorsApplication(ctx, executor)
		if err != nil {
			return "", "", err
		}
		if allPodsInSameAz {
			svc1log.FromContext(ctx).Info("Only considering nodes from the zone",
				svc1log.SafeParam("zone", zone))

			// It's gauranteed that there is a zone here
			availableNodes, err = filterNodesToZone(ctx, availableNodes, zone)
			if err != nil {
				return "", failureInternal, err
			}

			nodeNames = make([]string, 0, len(availableNodes))
			for _, node := range availableNodes {
				nodeNames = append(nodeNames, node.Name)
			}
			singleAzZone = zone
			shouldScheduleIntoSingleAZ = true
		} else {
			// It possible (and expected) to get here when the version of scheduler containing dynamic executor pods in the same zone is rolled out as previously scheduled applications may have executors in different AZs
			svc1log.FromContext(ctx).Info("Single AZ scheduling is enabled but could not locate a common AZ for scheduled pods, will attempt to schedule this pod in any AZ.")
		}
	} else {
		svc1log.FromContext(ctx).Info("Single AZ not enabled, attempting to schedule anywhere.")
	}

	usage := s.resourceReservationManager.GetReservedResources()
	overhead := s.overheadComputer.GetOverhead(ctx, availableNodes)
	availableNodesSchedulingMetadata := resources.NodeSchedulingMetadataForNodes(availableNodes, usage, overhead)

	usage.Add(overhead)
	availableResources := resources.AvailableForNodes(availableNodes, usage)

	name, reason, ok := s.findExecutorNode(
		availableNodesSchedulingMetadata,
		nodeNames,
		executorResources,
		availableResources,
		isExtraExecutor)
	if ok {
		return name, reason, nil
	}

	if shouldScheduleIntoSingleAZ {
		if s.binpacker.RequiredSingleAz {
			svc1log.FromContext(ctx).Info("Failed to find space in zone for additional executor, creating a demand", svc1log.SafeParam("zone", singleAzZone))
			metrics.IncrementSingleAzDynamicAllocationPackFailure(ctx, singleAzZone)
			demandZone := demandapi.Zone(singleAzZone)
			s.createDemandForExecutorInSpecificZone(ctx, executor, executorResources, &demandZone)
		} else {
			name, reason, ok = s.findExecutorNode(
				availableNodesSchedulingMetadata,
				initialNodeNames,
				executorResources,
				availableResources,
				isExtraExecutor,
			)
			if ok {
				svc1log.FromContext(ctx).Info("Preferred single az scheduling resulted in an executor being scheduled in a different AZ")
				return name, reason, nil
			}

			// we still create the demand in any AZ, as we want to maximize the chance of being able to provision a new
			// node, even if that means paying for cross-az traffic
			s.createDemandForExecutorInAnyZone(ctx, executor, executorResources)
		}
	} else {
		s.createDemandForExecutorInAnyZone(ctx, executor, executorResources)
	}
	return "", failureFit, werror.ErrorWithContextParams(ctx, "not enough capacity to reschedule the executor")
}

func (s *SparkSchedulerExtender) findExecutorNode(
	availableNodesSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	nodeNames []string,
	executorResources *resources.Resources,
	availableResources resources.NodeGroupResources,
	isExtraExecutor bool,
) (string, string, bool) {
	_, executorNodeNames := s.nodeSorter.PotentialNodes(availableNodesSchedulingMetadata, nodeNames)
	for _, name := range executorNodeNames {
		if !executorResources.GreaterThan(availableResources[name]) {
			if isExtraExecutor {
				return name, successScheduledExtraExecutor, true
			}
			return name, successRescheduled, true
		}
	}
	return "", failureFit, false
}

func (s *SparkSchedulerExtender) isSuccessOutcome(outcome string) bool {
	return outcome == success || outcome == successAlreadyBound || outcome == successRescheduled || outcome == successScheduledExtraExecutor
}
