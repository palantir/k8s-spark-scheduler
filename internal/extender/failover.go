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

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/internal"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
)

// SyncResourceReservationsAndDemands gets all resource reservations and pods,
// finds all pods that do not have a claimed spot in their resource reservation object,
// groups them by spark application id, and patched the resource reservation objects so
// they now reflect the current state of the world. This is needed on a leader failover,
// as async writes for resource reservation objects mean some writes will be lost on
// leader change, so the extender needs to call this before accepting requests.
func SyncResourceReservationsAndDemands(
	ctx context.Context,
	podLister corelisters.PodLister,
	nodeLister corelisters.NodeLister,
	resourceReservations *cache.ResourceReservationCache,
	demands *cache.DemandCache,
	overheadComputer *OverheadComputer) error {

	pods, err := podLister.List(labels.Everything())
	if err != nil {
		return err
	}
	nodes, err := nodeLister.List(labels.Everything())
	if err != nil {
		return err
	}
	rrs := resourceReservations.List()
	availableResources := availableResourcesPerInstanceGroup(ctx, rrs, nodes, overheadComputer.GetOverhead(ctx, nodes))
	staleSparkPods := sparkPodsByAppID(ctx, rrs, pods, nodeLister)

	for _, sp := range staleSparkPods {
		if sp.rr != nil {
			// resource reservation was created, but does not have all executors in its status
			patchedRR := syncResourceReservation(ctx, sp, podLister)
			resourceReservations.Update(patchedRR)
		} else {
			newRR, newAvailableResources := createResourceReservation(sp, availableResources[sp.instanceGroup])
			availableResources[sp.instanceGroup] = newAvailableResources
			// resource reservation was never created, need to re-binpack executors
			resourceReservations.Create(newRR)
		}
		// TODO: delete demands if they exist
	}
	// recompute overhead to accommodate for newly created resource reservations
	overheadComputer.compute(ctx)
	return nil
}

// sparkPods is a collection of stale state, it is comprised of
// pods from a spark application that do not have a claimed resource reservation,
// and the last known state of the resource reservation object
type sparkPods struct {
	driver        *v1.Pod
	executors     []*v1.Pod
	rr            *v1beta1.ResourceReservation
	instanceGroup string
}

func sparkPodsByAppID(
	ctx context.Context,
	rrs []*v1beta1.ResourceReservation,
	pods []*v1.Pod,
	nodeLister corelisters.NodeLister,
) map[string]*sparkPods {
	podsWithRRs := make(map[string]bool, len(rrs))
	appIDToRR := make(map[string]*v1beta1.ResourceReservation)
	for _, rr := range rrs {
		appIDToRR[rr.Labels[v1beta1.AppIDLabel]] = rr
		for _, podName := range rr.Status.Pods {
			podsWithRRs[podName] = true
		}
	}

	appIDToPods := make(map[string]*sparkPods)
	for _, pod := range pods {
		if pod.Spec.SchedulerName != SparkSchedulerName ||
			pod.DeletionTimestamp != nil ||
			pod.Spec.NodeName == "" ||
			podsWithRRs[pod.Name] {
			// count only spark scheduler managed pods without resource reservations
			continue
		}
		appID := pod.Labels[SparkAppIDLabel]
		node, err := nodeLister.Get(pod.Spec.NodeName)
		if err != nil {
			svc1log.FromContext(ctx).Warn("node does not exist in cache",
				svc1log.SafeParam("nodeName", pod.Spec.NodeName),
				svc1log.SafeParams(internal.PodSafeParams(*pod)))
			continue
		}
		sp, ok := appIDToPods[appID]
		if !ok {
			sp = &sparkPods{
				rr:            appIDToRR[appID],
				instanceGroup: node.Labels[instanceGroupNodeSelector],
			}
		}
		switch pod.Labels[SparkRoleLabel] {
		case Driver:
			sp.driver = pod
		case Executor:
			sp.executors = append(sp.executors, pod)
		default:
			svc1log.FromContext(ctx).Error("received non spark pod, ignoring", svc1log.SafeParams(internal.PodSafeParams(*pod)))
		}
		appIDToPods[appID] = sp
	}
	return appIDToPods
}

func availableResourcesPerInstanceGroup(
	ctx context.Context,
	rrs []*v1beta1.ResourceReservation,
	nodes []*v1.Node,
	overhead resources.NodeGroupResources) map[string]resources.NodeGroupResources {
	schedulableNodes := make(map[string][]*v1.Node)
	for _, n := range nodes {
		if n.Spec.Unschedulable {
			continue
		}
		instanceGroup := n.Labels[instanceGroupNodeSelector]
		schedulableNodes[instanceGroup] = append(schedulableNodes[instanceGroup], n)
	}
	usages := resources.UsageForNodes(rrs)
	usages.Add(overhead)
	availableResources := make(map[string]resources.NodeGroupResources)
	for instanceGroup, ns := range schedulableNodes {
		availableResources[instanceGroup] = resources.AvailableForNodes(ns, usages)
	}
	return availableResources
}

// syncResourceReservation gets a stale resource reservation and updates its status to reflect all pods in the given sparkPods object
func syncResourceReservation(ctx context.Context, sparkPods *sparkPods, podLister corelisters.PodLister) *v1beta1.ResourceReservation {
	if sparkPods.driver != nil {
		svc1log.FromContext(ctx).Error("resource reservation does not have a driver entry",
			svc1log.SafeParams(internal.PodSafeParams(*sparkPods.driver)))
		sparkPods.rr.Status.Pods["driver"] = sparkPods.driver.Name
	}
	for _, e := range sparkPods.executors {
		for name, r := range sparkPods.rr.Spec.Reservations {
			if r.Node != e.Spec.NodeName {
				continue
			}
			currentPodName, ok := sparkPods.rr.Status.Pods[name]
			if !ok {
				sparkPods.rr.Status.Pods[name] = e.Name
				break
			}
			pod, err := podLister.Pods(e.Namespace).Get(currentPodName)
			if errors.IsNotFound(err) || (err == nil && isPodTerminated(pod)) {
				sparkPods.rr.Status.Pods[name] = e.Name
				break
			}
		}
	}
	return sparkPods.rr
}

func createResourceReservation(
	sparkPods *sparkPods,
	availableResources resources.NodeGroupResources) (*v1beta1.ResourceReservation, resources.NodeGroupResources) {
	return nil, nil
}
