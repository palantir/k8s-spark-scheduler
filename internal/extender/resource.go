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
	"sort"
	"time"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	demandclient "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/scaler/v1alpha1"
	sparkschedulerclient "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/typed/sparkscheduler/v1beta1"
	sparkschedulerlisters "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/listers/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/logging"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/internal"
	"github.com/palantir/k8s-spark-scheduler/internal/metrics"
	"github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"go.uber.org/atomic"
	"k8s.io/api/core/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api"
)

const (
	failureUnbound      = "failure-unbound"
	failureInternal     = "failure-internal"
	failureFit          = "failure-fit"
	failureNonSparkPod  = "failure-non-spark-pod"
	success             = "success"
	successRescheduled  = "success-rescheduled"
	successAlreadyBound = "success-already-bound"
)

// SparkSchedulerExtender is a kubernetes scheduler extended responsible for ensuring
// a spark driver and all of the executors can be scheduled together given current
// resources available across the nodes
type SparkSchedulerExtender struct {
	nodeLister                corelisters.NodeLister
	podLister                 *SparkPodLister
	resourceReservationLister sparkschedulerlisters.ResourceReservationLister
	resourceReservationClient sparkschedulerclient.SparkschedulerV1beta1Interface
	coreClient                corev1.CoreV1Interface

	demandClient        demandclient.ScalerV1alpha1Interface
	apiExtensionsClient apiextensionsclientset.Interface

	demandCRDInitialized atomic.Bool

	isFIFO           bool
	binpacker        *binpacker
	overheadComputer *OverheadComputer
}

// NewExtender is responsible for creating and initializing a SparkSchedulerExtender
func NewExtender(
	nodeLister corelisters.NodeLister,
	podLister *SparkPodLister,
	resourceReservationLister sparkschedulerlisters.ResourceReservationLister,
	resourceReservationClient sparkschedulerclient.SparkschedulerV1beta1Interface,
	coreClient corev1.CoreV1Interface,
	demandClient demandclient.ScalerV1alpha1Interface,
	apiExtensionsClient apiextensionsclientset.Interface,
	isFIFO bool,
	binpackAlgo string,
	overheadComputer *OverheadComputer) *SparkSchedulerExtender {
	return &SparkSchedulerExtender{
		nodeLister:                nodeLister,
		podLister:                 podLister,
		resourceReservationLister: resourceReservationLister,
		resourceReservationClient: resourceReservationClient,
		coreClient:                coreClient,
		demandClient:              demandClient,
		apiExtensionsClient:       apiExtensionsClient,
		isFIFO:                    isFIFO,
		binpacker:                 selectBinpacker(binpackAlgo),
		overheadComputer:          overheadComputer,
	}
}

// Start is responsible for starting background goroutines for the SparkSchedulerExtender
func (s *SparkSchedulerExtender) Start(ctx context.Context) {
	if s.checkDemandCRDExists(ctx) {
		return
	}
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if s.checkDemandCRDExists(ctx) {
					return
				}
			}
		}
	}()
}

// Predicate is responsible for returning a filtered list of nodes that qualify to schedule the pod provided in the
// ExtenderArgs
func (s *SparkSchedulerExtender) Predicate(ctx context.Context, args schedulerapi.ExtenderArgs) *schedulerapi.ExtenderFilterResult {
	params := internal.PodSafeParams(args.Pod)
	role := args.Pod.Labels[SparkRoleLabel]
	params["podSparkRole"] = role
	params["instanceGroup"] = args.Pod.Spec.NodeSelector[instanceGroupNodeSelector]
	ctx = svc1log.WithLoggerParams(ctx, svc1log.SafeParams(params))
	logger := svc1log.FromContext(ctx)

	timer := metrics.NewScheduleTimer(ctx, &args.Pod)
	logger.Info("starting scheduling pod")
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
// adds their resource usage to availableResources
func (s *SparkSchedulerExtender) fitEarlierDrivers(
	ctx context.Context,
	drivers []*v1.Pod,
	nodeNames, executorNodeNames []string,
	availableResources resources.NodeGroupResources) bool {
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
			applicationResources.executorCount,
			nodeNames, executorNodeNames, availableResources)
		if !hasCapacity {
			svc1log.FromContext(ctx).Warn("failed to fit one of the earlier drivers",
				svc1log.SafeParam("earlierDriverName", driver.Name))
			return false
		}
		availableResources.Sub(sparkResourceUsage(
			applicationResources.driverResources,
			applicationResources.executorResources,
			driverNode, executorNodes))
	}
	return true
}

func (s *SparkSchedulerExtender) selectDriverNode(ctx context.Context, driver *v1.Pod, nodeNames []string) (string, string, error) {
	availableNodes, err := s.nodeLister.List(labels.Set(driver.Spec.NodeSelector).AsSelector())
	if err != nil {
		return "", failureFit, err
	}

	driverNodeNames, executorNodeNames := s.potentialNodes(availableNodes, driver, nodeNames)
	usages, err := s.usedResources(driverNodeNames)
	if err != nil {
		return "", failureInternal, err
	}
	usages.Add(s.overheadComputer.GetOverhead(ctx, availableNodes))
	availableResources := resources.AvailableForNodes(availableNodes, usages)
	if s.isFIFO {
		queuedDrivers, err := s.podLister.ListEarlierDrivers(driver)
		if err != nil {
			return "", failureInternal, werror.Wrap(err, "failed to list earlier drivers")
		}
		ok := s.fitEarlierDrivers(ctx, queuedDrivers, driverNodeNames, executorNodeNames, availableResources)
		if !ok {
			return "", failureFit, werror.Error("earlier drivers do not fit to the cluster")
		}
	}
	applicationResources, err := sparkResources(ctx, driver)
	if err != nil {
		return "", failureInternal, werror.Wrap(err, "failed to get spark resources")
	}
	driverNode, executorNodes, hasCapacity := s.binpacker.BinpackFunc(
		ctx,
		applicationResources.driverResources,
		applicationResources.executorResources,
		applicationResources.executorCount,
		driverNodeNames, executorNodeNames, availableResources)
	svc1log.FromContext(ctx).Debug("binpacking result",
		svc1log.SafeParam("availableResources", availableResources),
		svc1log.SafeParam("driverResources", applicationResources.driverResources),
		svc1log.SafeParam("executorResources", applicationResources.executorResources),
		svc1log.SafeParam("executorCount", applicationResources.executorCount),
		svc1log.SafeParam("hasCapacity", hasCapacity),
		svc1log.SafeParam("candidateDriverNodes", nodeNames),
		svc1log.SafeParam("candidateExecutorNodes", executorNodeNames),
		svc1log.SafeParam("driverNode", driverNode),
		svc1log.SafeParam("executorNodes", executorNodes),
		svc1log.SafeParam("binpacker", s.binpacker.Name))
	if !hasCapacity {
		if err := s.createDemandForApplication(ctx, driver, applicationResources); err != nil {
			return "", failureInternal, werror.Wrap(err, "application does not fit to the cluster, but failed to create demand resource")
		}
		return "", failureFit, werror.Error("application does not fit to the cluster")
	}
	s.removeDemandIfExists(ctx, driver)
	return s.createResourceReservations(ctx, driver, applicationResources, driverNode, executorNodes)
}

func (s *SparkSchedulerExtender) potentialNodes(availableNodes []*v1.Node, driver *v1.Pod, nodeNames []string) (driverNodes, executorNodes []string) {
	sort.Slice(availableNodes, func(i, j int) bool {
		return availableNodes[j].CreationTimestamp.Before(&availableNodes[i].CreationTimestamp)
	})

	driverNodeNames := make([]string, 0, len(availableNodes))
	executorNodeNames := make([]string, 0, len(availableNodes))

	nodeNamesSet := make(map[string]interface{})
	for _, item := range nodeNames {
		nodeNamesSet[item] = nil
	}

	for _, node := range availableNodes {
		if _, ok:= nodeNamesSet[node.Name]; ok {
			driverNodeNames = append(driverNodeNames, node.Name)
		}
		if !node.Spec.Unschedulable {
			executorNodeNames = append(executorNodeNames, node.Name)
		}
	}
	return driverNodeNames, executorNodeNames
}

func (s *SparkSchedulerExtender) selectExecutorNode(ctx context.Context, executor *v1.Pod, nodeNames []string) (string, string, error) {
	resourceReservation, err := s.resourceReservationLister.ResourceReservations(executor.Namespace).Get(executor.Labels[SparkAppIDLabel])
	if err != nil {
		return "", failureInternal, werror.Wrap(err, "failed to get resource reservations")
	}
	unboundReservations, outcome, err := s.findUnboundReservations(ctx, executor, resourceReservation)
	if err != nil {
		return "", outcome, err
	}
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
		allNodesGone, err := s.allNodesGoneOrUnschedulable(ctx, unboundReservations, resourceReservation)
		if err != nil {
			return "", failureInternal, werror.Wrap(err, "failed to get nodes")
		}
		if !allNodesGone {
			svc1log.FromContext(ctx).Info("there are live nodes that we are not receiving, will try reschedule")
		}
		// we are guaranteed len(unboundResourceReservations) > 0
		unboundReservation = unboundReservations[0]
		node, outcome, err := s.rescheduleExecutor(ctx, executor, nodeNames, unboundReservation, resourceReservation)
		if err != nil {
			return "", outcome, err
		}
		svc1log.FromContext(ctx).Info("rescheduling executor", svc1log.SafeParam("reservationName", unboundReservation), svc1log.SafeParam("node", node))
		reservation := copyResourceReservation.Spec.Reservations[unboundReservation]
		reservation.Node = node
		copyResourceReservation.Spec.Reservations[unboundReservation] = reservation
	}

	copyResourceReservation.Status.Pods[unboundReservation] = executor.Name
	_, err = s.resourceReservationClient.ResourceReservations(copyResourceReservation.Namespace).Update(copyResourceReservation)
	if err != nil {
		_, createErr := s.resourceReservationClient.ResourceReservations(copyResourceReservation.Namespace).Create(copyResourceReservation)
		if createErr != nil {
			if errors.IsAlreadyExists(createErr) {
				return "", failureInternal, werror.Wrap(err, "failed to update resource reservation")
			}
			return "", failureInternal, werror.Wrap(createErr, "failed to create v1beta1 resource reservation")
		}
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

func (s *SparkSchedulerExtender) usedResources(nodeNames []string) (resources.NodeGroupResources, error) {
	// TODO: add instancegroup tag after migration
	resourceReservations, err := s.resourceReservationLister.List(labels.Everything())
	if err != nil {
		return nil, werror.Wrap(err, "Failed to list resource resevations")
	}
	return resources.UsageForNodes(resourceReservations), nil
}

func (s *SparkSchedulerExtender) createResourceReservations(
	ctx context.Context,
	driver *v1.Pod,
	applicationResources *sparkApplicationResources,
	driverNode string,
	executorNodes []string) (string, string, error) {
	logger := svc1log.FromContext(ctx)
	rr := newResourceReservation(driverNode, executorNodes, driver, applicationResources.driverResources, applicationResources.executorResources)
	_, err := s.resourceReservationClient.ResourceReservations(driver.Namespace).Create(rr)
	if err != nil {
		if !errors.IsAlreadyExists(err) {
			return "", failureInternal, werror.Wrap(err, "failed to create driver resource reservation")
		}
		existingRR, getErr := s.resourceReservationClient.ResourceReservations(driver.Namespace).Get(rr.Name, metav1.GetOptions{})
		if getErr != nil {
			return "", failureInternal, werror.Wrap(getErr, "failed to get existing resource reservation")
		}
		return existingRR.Spec.Reservations["driver"].Node, success, nil
	}
	logger.Debug("creating executor resource reservations", svc1log.SafeParams(logging.RRSafeParam(rr)))
	return driverNode, success, nil
}

func (s *SparkSchedulerExtender) rescheduleExecutor(ctx context.Context, executor *v1.Pod, nodeNames []string, reservationName string, rr *v1beta1.ResourceReservation) (string, string, error) {
	reservation := rr.Spec.Reservations[reservationName]
	executorResources := &resources.Resources{CPU: reservation.CPU, Memory: reservation.Memory}
	availableNodes := s.getNodes(ctx, nodeNames)
	usages, err := s.usedResources(nodeNames)
	if err != nil {
		return "", failureInternal, err
	}
	usages.Add(s.overheadComputer.GetOverhead(ctx, availableNodes))
	availableResources := resources.AvailableForNodes(availableNodes, usages)
	for _, name := range nodeNames {
		if !executorResources.GreaterThan(availableResources[name]) {
			return name, successRescheduled, nil
		}
	}
	if err := s.createDemandForExecutor(ctx, executor, executorResources); err != nil {
		return "", failureInternal, werror.Wrap(err, "executor does not fit to the cluster, but failed to create demand resource")
	}
	return "", failureFit, werror.Error("not enough capacity to reschedule the executor")
}

// if err is nil, it is guaranteed to return a non empty array of resource reservations
func (s *SparkSchedulerExtender) findUnboundReservations(ctx context.Context, executor *v1.Pod, resourceReservation *v1beta1.ResourceReservation) ([]string, string, error) {
	unboundReservations := make([]string, 0, len(resourceReservation.Spec.Reservations))
	for name := range resourceReservation.Spec.Reservations {
		podName, ok := resourceReservation.Status.Pods[name]
		if !ok {
			unboundReservations = append(unboundReservations, name)
		}
		if podName == executor.Name {
			svc1log.FromContext(ctx).Info("found already bound resource reservation for the current pod", svc1log.SafeParam("reservationName", name))
			return []string{name}, successAlreadyBound, nil
		}
	}
	if len(unboundReservations) > 0 {
		return unboundReservations, success, nil
	}
	selector := labels.Set(map[string]string{SparkAppIDLabel: executor.Labels[SparkAppIDLabel]}).AsSelector()
	pods, err := s.podLister.Pods(executor.Namespace).List(selector)
	if err != nil {
		return nil, failureInternal, werror.Wrap(err, "failed to list pods")
	}
	podNames := make(map[string]bool, len(pods))
	for _, pod := range pods {
		podNames[pod.Name] = true
	}
	relocatableReservations := make([]string, 0, len(resourceReservation.Spec.Reservations))
	for name := range resourceReservation.Spec.Reservations {
		podIdentifier := resourceReservation.Status.Pods[name]
		if !podNames[podIdentifier] {
			relocatableReservations = append(relocatableReservations, name)
		}
	}

	if len(relocatableReservations) == 0 {
		return nil, failureUnbound, werror.Error("failed to find unbound resource reservation", werror.SafeParams(logging.RRSafeParam(resourceReservation)))
	}
	svc1log.FromContext(ctx).Info("found relocatable resource reservations", svc1log.SafeParams(logging.RRSafeParam(resourceReservation)))
	return relocatableReservations, successRescheduled, nil
}

func (s *SparkSchedulerExtender) allNodesGoneOrUnschedulable(ctx context.Context, unboundReservations []string, resourceReservation *v1beta1.ResourceReservation) (bool, error) {
	liveNodes := make([]string, 0, len(unboundReservations))
	unschedulableNodes := make([]string, 0, len(unboundReservations))
	goneNodes := make([]string, 0, len(unboundReservations))
	for _, reservationName := range unboundReservations {
		nodeName := resourceReservation.Spec.Reservations[reservationName].Node
		nodeObj, err := s.nodeLister.Get(nodeName)
		if err != nil {
			if errors.IsNotFound(err) {
				goneNodes = append(goneNodes, nodeName)
			} else {
				return false, err
			}
			continue
		}
		if nodeObj.Spec.Unschedulable {
			unschedulableNodes = append(unschedulableNodes, nodeName)
		} else {
			liveNodes = append(liveNodes, nodeName)
		}
	}
	svc1log.FromContext(ctx).Info("node status of unbound resource reservations",
		svc1log.SafeParam("goneNodes", goneNodes),
		svc1log.SafeParam("liveNodes", liveNodes),
		svc1log.SafeParam("unschedulableNodes", unschedulableNodes))
	return len(liveNodes) == 0, nil
}

func (s *SparkSchedulerExtender) deleteResourceReservation(namespace, name string) error {
	background := metav1.DeletePropagationBackground
	return s.resourceReservationClient.ResourceReservations(namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &background})
}
