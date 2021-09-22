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
	"math"
	"sort"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta2"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/internal"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	"github.com/palantir/k8s-spark-scheduler/internal/common/utils"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
)

// SyncResourceReservationsAndDemands gets all resource reservations and pods,
// finds all pods that do not have a claimed spot in their resource reservation object,
// groups them by spark application id, and patches the resource reservation objects so
// they now reflect the current state of the world. This is needed on a leader failover,
// as async writes for resource reservation objects mean some writes will be lost on
// leader change, so the extender needs to call this before accepting requests.
func (s *SparkSchedulerExtender) syncResourceReservationsAndDemands(ctx context.Context) error {
	pods, err := s.podLister.List(labels.Everything())
	if err != nil {
		return err
	}
	nodes, err := s.nodeLister.List(labels.Everything())
	if err != nil {
		return err
	}
	rrs := s.resourceReservations.List()
	overhead := s.overheadComputer.GetOverhead(ctx, nodes)
	softReservationOverhead := s.softReservationStore.UsedSoftReservationResources()
	availableResources, orderedNodes := availableResourcesPerInstanceGroup(s.instanceGroupLabel, rrs, nodes, overhead, softReservationOverhead)
	staleSparkPods := unreservedSparkPodsBySparkID(ctx, rrs, s.softReservationStore, pods)
	svc1log.FromContext(ctx).Info("starting reconciliation", svc1log.SafeParam("appCount", len(staleSparkPods)))

	r := &reconciler{s.podLister, s.resourceReservations, s.softReservationStore, s.demands, availableResources, orderedNodes, s.instanceGroupLabel}

	extraExecutorsWithNoRRs := make(map[string][]*v1.Pod)
	for _, sp := range staleSparkPods {
		extraExecutors := r.syncResourceReservations(ctx, sp)
		if len(extraExecutors) > 0 {
			extraExecutorsWithNoRRs[sp.appID] = extraExecutors
		}
		r.syncDemands(ctx, sp)
	}
	err = r.syncSoftReservations(ctx, extraExecutorsWithNoRRs)
	if err != nil {
		return nil
	}
	return nil
}

// sparkPods is a collection of stale state, it is comprised of
// pods from a spark application that do not have a claimed resource reservation,
// and the last known state of the resource reservation object
type sparkPods struct {
	appID                 string
	inconsistentDriver    *v1.Pod
	inconsistentExecutors []*v1.Pod
}

type instanceGroup string

type reconciler struct {
	podLister            *SparkPodLister
	resourceReservations *cache.ResourceReservationCache
	softReservations     *cache.SoftReservationStore
	demands              *cache.SafeDemandCache
	availableResources   map[instanceGroup]resources.NodeGroupResources
	orderedNodes         map[instanceGroup][]*v1.Node
	instanceGroupLabel   string
}

func (r *reconciler) syncResourceReservations(ctx context.Context, sp *sparkPods) []*v1.Pod {
	extraExecutors := make([]*v1.Pod, 0, len(sp.inconsistentExecutors))
	if sp.inconsistentDriver == nil && len(sp.inconsistentExecutors) > 0 {
		// if the driver is nil it already has an associated reservation, get the resource
		// reservation object and update it so it has reservations for each stale executor
		exec := sp.inconsistentExecutors[0]
		rr, ok := r.resourceReservations.Get(exec.Namespace, sp.appID)
		if !ok {
			logRR(ctx, "resource reservation deleted, ignoring", exec.Namespace, sp.appID)
			return nil
		}
		newRR, err := r.patchResourceReservation(sp.inconsistentExecutors, rr.DeepCopy())
		if err != nil {
			logRR(ctx, "resource reservation deleted, ignoring", exec.Namespace, sp.appID)
			return nil
		}

		podsWithRR := make(map[string]bool, len(newRR.Status.Pods))
		for _, podName := range newRR.Status.Pods {
			podsWithRR[podName] = true
		}
		for _, executor := range sp.inconsistentExecutors {
			if _, ok := podsWithRR[executor.Name]; !ok {
				extraExecutors = append(extraExecutors, executor)
			}
		}
	} else if sp.inconsistentDriver != nil {
		// the driver is stale, a new resource reservation object needs to be created
		appResources, err := r.getAppResources(ctx, sp)
		if err != nil {
			svc1log.FromContext(ctx).Error("could not get application resources for application",
				svc1log.SafeParam("appID", sp.appID), svc1log.Stacktrace(err))
			return nil
		}
		ig, _ := internal.FindInstanceGroupFromPodSpec(sp.inconsistentDriver.Spec, r.instanceGroupLabel)
		instanceGroup := instanceGroup(ig)
		endIdx := int(math.Min(float64(len(sp.inconsistentExecutors)), float64(appResources.minExecutorCount)))
		executorsUpToMin := sp.inconsistentExecutors[0:endIdx]
		extraExecutors = sp.inconsistentExecutors[endIdx:]

		newRR, reservedResources, err := r.constructResourceReservation(ctx, sp.inconsistentDriver, executorsUpToMin, instanceGroup)
		if err != nil {
			svc1log.FromContext(ctx).Error("failed to construct resource reservation", svc1log.Stacktrace(err))
			return nil
		}
		err = r.resourceReservations.Create(newRR)
		if err != nil {
			logRR(ctx, "resource reservation already exists, force updating", sp.inconsistentDriver.Namespace, sp.appID)
			updateErr := r.resourceReservations.Update(newRR)
			if updateErr != nil {
				logRR(ctx, "resource reservation deleted, ignoring", sp.inconsistentDriver.Namespace, sp.appID)
				return nil
			}
		}
		r.availableResources[instanceGroup].Sub(reservedResources)
	}

	return extraExecutors
}

func (r *reconciler) syncDemands(ctx context.Context, sp *sparkPods) {
	if sp.inconsistentDriver != nil {
		DeleteDemandIfExists(ctx, r.demands, sp.inconsistentDriver, "Reconciler")
	}
	for _, e := range sp.inconsistentExecutors {
		DeleteDemandIfExists(ctx, r.demands, e, "Reconciler")
	}
}

func (r *reconciler) syncSoftReservations(ctx context.Context, extraExecutorsByApp map[string][]*v1.Pod) error {
	// Initialize SoftReservationStore with dynamic allocation applications currently running
	err := r.syncApplicationSoftReservations(ctx)
	if err != nil {
		return err
	}

	// Sync executors
	for appID, extraExecutors := range extraExecutorsByApp {
		driver, err := r.podLister.getDriverPodForExecutor(ctx, extraExecutors[0])
		if err != nil {
			svc1log.FromContext(ctx).Error("Error getting driver pod for executor, skipping...", svc1log.SafeParam("appID", appID), svc1log.Stacktrace(err))
			continue
		}
		applicationResources, err := sparkResources(ctx, driver)
		if err != nil {
			svc1log.FromContext(ctx).Error("Error getting spark resources for application, skipping...", svc1log.SafeParam("appID", appID), svc1log.Stacktrace(err))
			continue
		}

		for i, extraExecutor := range extraExecutors {
			if i >= (applicationResources.maxExecutorCount - applicationResources.minExecutorCount) {
				break
			}
			err := r.softReservations.AddReservationForPod(ctx, appID, extraExecutor.Name, v1beta2.Reservation{
				Node: extraExecutor.Spec.NodeName,
				Resources: v1beta2.ResourceList{
					string(v1beta2.ResourceCPU):    &applicationResources.executorResources.CPU,
					string(v1beta2.ResourceMemory): &applicationResources.executorResources.Memory,
				},
			})
			if err != nil {
				svc1log.FromContext(ctx).Error("failed to add soft reservation for executor on failover. skipping...", svc1log.SafeParam("appID", appID), svc1log.Stacktrace(err))
			}
		}
	}
	return nil
}

// syncApplicationSoftReservations creates empty SoftReservations for all applications that can have extra executors in dynamic allocation
// in order to prefill the SoftReservationStore with the drivers currently running
func (r *reconciler) syncApplicationSoftReservations(ctx context.Context) error {
	selector := labels.Set(map[string]string{common.SparkRoleLabel: common.Driver}).AsSelector()
	drivers, err := r.podLister.List(selector)
	if err != nil {
		return werror.Wrap(err, "failed to list drivers")
	}

	for _, d := range drivers {
		if d.Spec.SchedulerName != common.SparkSchedulerName || d.Spec.NodeName == "" || d.Status.Phase == v1.PodSucceeded || d.Status.Phase == v1.PodFailed {
			continue
		}
		appResources, err := sparkResources(ctx, d)
		if err != nil {
			svc1log.FromContext(ctx).Error("failed to get driver resources, skipping driver",
				svc1log.SafeParam("faultyDriverName", d.Name),
				svc1log.SafeParam("reason", err.Error),
				svc1log.Stacktrace(err))
			continue
		}

		if appResources.maxExecutorCount > appResources.minExecutorCount {
			r.softReservations.CreateSoftReservationIfNotExists(d.Labels[common.SparkAppIDLabel])
		}
	}
	return nil
}

func unreservedSparkPodsBySparkID(
	ctx context.Context,
	rrs []*v1beta2.ResourceReservation,
	softReservationStore *cache.SoftReservationStore,
	pods []*v1.Pod,
) map[string]*sparkPods {
	podsWithRRs := make(map[string]bool, len(rrs))
	for _, rr := range rrs {
		for _, podName := range rr.Status.Pods {
			podsWithRRs[podName] = true
		}
	}

	appIDToPods := make(map[string]*sparkPods)
	for _, pod := range pods {
		if isNotScheduledSparkPod(pod) || podsWithRRs[pod.Name] ||
			(pod.Labels[common.SparkRoleLabel] == common.Executor && softReservationStore.ExecutorHasSoftReservation(ctx, pod)) {
			continue
		}
		appID := pod.Labels[common.SparkAppIDLabel]
		sp, ok := appIDToPods[appID]
		if !ok {
			sp = &sparkPods{
				appID: appID,
			}
		}
		switch pod.Labels[common.SparkRoleLabel] {
		case common.Driver:
			sp.inconsistentDriver = pod
		case common.Executor:
			sp.inconsistentExecutors = append(sp.inconsistentExecutors, pod)
		default:
			svc1log.FromContext(ctx).Error("received non spark pod, ignoring", svc1log.SafeParams(internal.PodSafeParams(*pod)))
		}
		appIDToPods[appID] = sp
	}
	return appIDToPods
}

func isNotScheduledSparkPod(pod *v1.Pod) bool {
	return pod.Spec.SchedulerName != common.SparkSchedulerName || pod.DeletionTimestamp != nil || pod.Spec.NodeName == ""
}

func availableResourcesPerInstanceGroup(
	instanceGroupLabel string,
	rrs []*v1beta2.ResourceReservation,
	nodes []*v1.Node,
	overhead resources.NodeGroupResources,
	softReservationOverhead resources.NodeGroupResources) (map[instanceGroup]resources.NodeGroupResources, map[instanceGroup][]*v1.Node) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[j].CreationTimestamp.Before(&nodes[i].CreationTimestamp)
	})

	schedulableNodes := make(map[instanceGroup][]*v1.Node)
	for _, n := range nodes {
		if n.Spec.Unschedulable {
			continue
		}

		nodeReady := false
		for _, n := range n.Status.Conditions {
			if n.Type == v1.NodeReady && n.Status == v1.ConditionTrue {
				nodeReady = true
			}
		}
		if !nodeReady {
			continue
		}

		instanceGroup := instanceGroup(n.Labels[instanceGroupLabel])
		schedulableNodes[instanceGroup] = append(schedulableNodes[instanceGroup], n)
	}
	usages := resources.UsageForNodesV1Beta2(rrs)
	usages.Add(overhead)
	usages.Add(softReservationOverhead)
	availableResources := make(map[instanceGroup]resources.NodeGroupResources)
	for instanceGroup, ns := range schedulableNodes {
		availableResources[instanceGroup] = resources.AvailableForNodes(ns, usages)
	}
	return availableResources, schedulableNodes
}

// patchResourceReservation gets a stale resource reservation and updates its status to reflect all given executors
func (r *reconciler) patchResourceReservation(execs []*v1.Pod, rr *v1beta2.ResourceReservation) (*v1beta2.ResourceReservation, error) {
	for _, e := range execs {
		for name, reservation := range rr.Spec.Reservations {
			if reservation.Node != e.Spec.NodeName {
				continue
			}
			currentPodName, ok := rr.Status.Pods[name]
			if !ok {
				rr.Status.Pods[name] = e.Name
				break
			}
			pod, err := r.podLister.Pods(e.Namespace).Get(currentPodName)
			if errors.IsNotFound(err) || (err == nil && utils.IsPodTerminated(pod)) {
				rr.Status.Pods[name] = e.Name
				break
			}
		}
	}

	return rr, r.resourceReservations.Update(rr)
}

func (r *reconciler) constructResourceReservation(
	ctx context.Context,
	driver *v1.Pod,
	executors []*v1.Pod,
	instanceGroup instanceGroup) (*v1beta2.ResourceReservation, resources.NodeGroupResources, error) {
	applicationResources, err := sparkResources(ctx, driver)
	if err != nil {
		return nil, nil, err
	}

	nodes, nodesFound := r.orderedNodes[instanceGroup]
	availableResources, resourcesFound := r.availableResources[instanceGroup]
	if !nodesFound || !resourcesFound {
		return nil, nil, werror.Error("instance group not found", werror.SafeParam("instanceGroup", instanceGroup))
	}

	var reservedNodeNames []string
	var reservedResources resources.NodeGroupResources
	executorCountToAssignNodes := applicationResources.minExecutorCount - len(executors)
	if executorCountToAssignNodes > 0 {
		reservedNodeNames, reservedResources = findNodes(executorCountToAssignNodes, applicationResources.executorResources, availableResources, nodes)
		if len(reservedNodeNames) < executorCountToAssignNodes {
			svc1log.FromContext(ctx).Error("could not reserve space for all executors",
				svc1log.SafeParams(internal.PodSafeParams(*driver)))
		}
	}

	executorNodes := make([]string, 0, applicationResources.minExecutorCount)
	for _, e := range executors {
		executorNodes = append(executorNodes, e.Spec.NodeName)
	}
	executorNodes = append(executorNodes, reservedNodeNames...)
	rr := newResourceReservation(
		driver.Spec.NodeName,
		executorNodes,
		driver,
		applicationResources.driverResources,
		applicationResources.executorResources)
	for i, e := range executors {
		rr.Status.Pods[executorReservationName(i)] = e.Name
	}
	return rr, reservedResources, nil
}

func (r *reconciler) getAppResources(ctx context.Context, sp *sparkPods) (*sparkApplicationResources, error) {
	var driver *v1.Pod
	if sp.inconsistentDriver != nil {
		driver = sp.inconsistentDriver
	} else if len(sp.inconsistentExecutors) > 0 {
		d, err := r.podLister.getDriverPodForExecutor(ctx, sp.inconsistentExecutors[0])
		if err != nil {
			logRR(ctx, "error getting driver pod for executor", sp.inconsistentExecutors[0].Namespace, sp.appID)
			return nil, err
		}
		driver = d
	} else {
		return nil, werror.Error("no inconsistent driver or executor")
	}
	return sparkResources(ctx, driver)
}

// findNodes reserves space for n executors, picks nodes by the iterating
// through nodes with the given order.
// TODO: replace this with the binpack function once it can return partial results
func findNodes(
	executorCount int,
	executorResources *resources.Resources,
	availableResources resources.NodeGroupResources,
	orderedNodes []*v1.Node,
) ([]string, resources.NodeGroupResources) {
	executorNodeNames := make([]string, 0, executorCount)
	reserved := resources.NodeGroupResources{}
	for _, n := range orderedNodes {
		if reserved[n.Name] == nil {
			reserved[n.Name] = resources.Zero()
		}
		for {
			reserved[n.Name].Add(executorResources)
			if reserved[n.Name].GreaterThan(availableResources[n.Name]) {
				break
			}
			executorNodeNames = append(executorNodeNames, n.Name)
			if len(executorNodeNames) == executorCount {
				return executorNodeNames, reserved
			}
		}
	}
	return executorNodeNames, reserved
}

func logRR(ctx context.Context, msg, name, namespace string) {
	svc1log.FromContext(ctx).Error(msg,
		svc1log.SafeParam("resourceReservationNamespace", namespace),
		svc1log.SafeParam("resourceReservationName", name))
}
