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

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/internal"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
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
	softReservationOverhead := s.usedSoftReservationResources()
	availableResources, orderedNodes := availableResourcesPerInstanceGroup(ctx, rrs, nodes, overhead, softReservationOverhead)
	staleSparkPods := unreservedSparkPodsBySparkID(ctx, rrs, s.softReservationStore.GetAllSoftReservations(), pods)
	svc1log.FromContext(ctx).Info("starting reconciliation", svc1log.SafeParam("appCount", len(staleSparkPods)))

	r := &reconciler{s.podLister, s.resourceReservations, s.softReservationStore, s.demands, availableResources, orderedNodes}
	for _, sp := range staleSparkPods {
		r.syncResourceReservations(ctx, sp)
		r.syncDemand(ctx, sp)
	}
	// recompute overhead to account for newly created resource reservations
	s.overheadComputer.compute(ctx)
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
}

func (r *reconciler) syncResourceReservations(ctx context.Context, sp *sparkPods) {
	// if the driver is nil it already has an associated reservation, get the resource
	// reservation object and update it so it has reservations for each stale executor
	appResources, err := r.getAppResources(ctx, sp)
	if err != nil {
		return
	}
	extraExecutors := make([]*v1.Pod, 0, len(sp.inconsistentExecutors))

	if sp.inconsistentDriver == nil && len(sp.inconsistentExecutors) > 0 {
		exec := sp.inconsistentExecutors[0]
		rr, ok := r.resourceReservations.Get(exec.Namespace, sp.appID)
		if !ok {
			logRR(ctx, "resource reservation deleted, ignoring", exec.Namespace, sp.appID)
			return
		}
		unreservedExecutors, err := r.patchResourceReservation(sp.inconsistentExecutors, rr.DeepCopy())
		extraExecutors = unreservedExecutors
		if err != nil {
			logRR(ctx, "resource reservation deleted, ignoring", exec.Namespace, sp.appID)
			return
		}
	} else if sp.inconsistentDriver != nil {
		// the driver is stale, a new resource reservation object needs to be created
		instanceGroup := instanceGroup(sp.inconsistentDriver.Spec.NodeSelector[instanceGroupNodeSelector])
		endIdx := int(math.Min(float64(len(sp.inconsistentExecutors)), float64(appResources.minExecutorCount)))
		executorsUpToMin := sp.inconsistentExecutors[0:endIdx]
		extraExecutors = sp.inconsistentExecutors[endIdx:]

		newRR, reservedResources, err := r.constructResourceReservation(ctx, sp.inconsistentDriver, executorsUpToMin, instanceGroup)
		if err != nil {
			svc1log.FromContext(ctx).Error("failed to construct resource reservation", svc1log.Stacktrace(err))
			return
		}
		err = r.resourceReservations.Create(newRR)
		if err != nil {
			logRR(ctx, "resource reservation already exists, force updating", sp.inconsistentDriver.Namespace, sp.appID)
			updateErr := r.resourceReservations.Update(newRR)
			if updateErr != nil {
				logRR(ctx, "resource reservation deleted, ignoring", sp.inconsistentDriver.Namespace, sp.appID)
				return
			}
		}
		r.availableResources[instanceGroup].Sub(reservedResources)
	}

	// Create soft reservation object for drivers that can have extra executors even if they don't at the moment
	if appResources.maxExecutorCount > appResources.minExecutorCount {
		r.softReservations.CreateSoftReservationIfNotExists(sp.appID)
	}
	// Create soft reservations for the extra executors
	if len(extraExecutors) > 0 {
		for i, extraExecutor := range extraExecutors {
			if i >= (appResources.maxExecutorCount - appResources.minExecutorCount) {
				break
			}
			r.softReservations.AddReservationForPod(ctx, sp.appID, extraExecutor.Name, v1beta1.Reservation{
				Node:   extraExecutor.Spec.NodeName,
				CPU:    appResources.executorResources.CPU,
				Memory: appResources.executorResources.Memory,
			})
		}
	}

}

func (r *reconciler) syncDemand(ctx context.Context, sp *sparkPods) {
	if sp.inconsistentDriver != nil {
		r.deleteDemandIfExists(sp.inconsistentDriver.Namespace, demandResourceName(sp.inconsistentDriver))
	}
	for _, e := range sp.inconsistentExecutors {
		r.deleteDemandIfExists(e.Namespace, demandResourceName(e))
	}
}

func (r *reconciler) deleteDemandIfExists(namespace, name string) {
	_, ok := r.demands.Get(namespace, name)
	if ok {
		r.demands.Delete(namespace, name)
	}
}

func unreservedSparkPodsBySparkID(
	ctx context.Context,
	rrs []*v1beta1.ResourceReservation,
	softrrs map[string]*cache.SoftReservation,
	pods []*v1.Pod,
) map[string]*sparkPods {
	podsWithRRs := make(map[string]bool, len(rrs))
	for _, rr := range rrs {
		for _, podName := range rr.Status.Pods {
			podsWithRRs[podName] = true
		}
	}

	podsWithSoftRRs := make(map[string]bool, len(softrrs))
	for _, srr := range softrrs {
		for podName := range srr.Reservations {
			podsWithSoftRRs[podName] = true
		}
	}

	appIDToPods := make(map[string]*sparkPods)
	for _, pod := range pods {
		if isNotScheduledSparkPod(pod) || podsWithRRs[pod.Name] || podsWithSoftRRs[pod.Name] {
			continue
		}
		appID := pod.Labels[SparkAppIDLabel]
		sp, ok := appIDToPods[appID]
		if !ok {
			sp = &sparkPods{
				appID: appID,
			}
		}
		switch pod.Labels[SparkRoleLabel] {
		case Driver:
			sp.inconsistentDriver = pod
		case Executor:
			sp.inconsistentExecutors = append(sp.inconsistentExecutors, pod)
		default:
			svc1log.FromContext(ctx).Error("received non spark pod, ignoring", svc1log.SafeParams(internal.PodSafeParams(*pod)))
		}
		appIDToPods[appID] = sp
	}
	return appIDToPods
}

func isNotScheduledSparkPod(pod *v1.Pod) bool {
	return pod.Spec.SchedulerName != SparkSchedulerName || pod.DeletionTimestamp != nil || pod.Spec.NodeName == ""
}

func availableResourcesPerInstanceGroup(
	ctx context.Context,
	rrs []*v1beta1.ResourceReservation,
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
		instanceGroup := instanceGroup(n.Labels[instanceGroupNodeSelector])
		schedulableNodes[instanceGroup] = append(schedulableNodes[instanceGroup], n)
	}
	usages := resources.UsageForNodes(rrs)
	usages.Add(overhead)
	usages.Add(softReservationOverhead)
	availableResources := make(map[instanceGroup]resources.NodeGroupResources)
	for instanceGroup, ns := range schedulableNodes {
		availableResources[instanceGroup] = resources.AvailableForNodes(ns, usages)
	}
	return availableResources, schedulableNodes
}

// patchResourceReservation gets a stale resource reservation and updates its status to reflect all given executors
func (r *reconciler) patchResourceReservation(execs []*v1.Pod, rr *v1beta1.ResourceReservation) ([]*v1.Pod, error) {
	extraExecutors := make([]*v1.Pod, 0, len(execs))
	for _, e := range execs {
		reservedForExecutor := false
		for name, reservation := range rr.Spec.Reservations {
			if reservation.Node != e.Spec.NodeName {
				continue
			}
			currentPodName, ok := rr.Status.Pods[name]
			if !ok {
				// TODO: is there an issue taking a reservation irrespective of order and who might have been there before failover?
				rr.Status.Pods[name] = e.Name
				reservedForExecutor = true
				break
			}
			pod, err := r.podLister.Pods(e.Namespace).Get(currentPodName)
			if errors.IsNotFound(err) || (err == nil && isPodTerminated(pod)) {
				rr.Status.Pods[name] = e.Name
				reservedForExecutor = true
				break
			}
		}

		if !reservedForExecutor {
			extraExecutors = append(extraExecutors, e)
		}
	}

	return extraExecutors, r.resourceReservations.Update(rr)
}

func (r *reconciler) constructResourceReservation(
	ctx context.Context,
	driver *v1.Pod,
	executors []*v1.Pod,
	instanceGroup instanceGroup) (*v1beta1.ResourceReservation, resources.NodeGroupResources, error) {
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
	for _, e := range executors[0:applicationResources.minExecutorCount] {
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
		d, err := r.podLister.getDriverPod(ctx, sp.inconsistentExecutors[0])
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
