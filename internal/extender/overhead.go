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
	"sync"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	"github.com/palantir/k8s-spark-scheduler/internal/common/utils"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	coreinformers "k8s.io/client-go/informers/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	clientcache "k8s.io/client-go/tools/cache"
)

// OverheadComputer computes non spark scheduler managed pods total resources periodically
type OverheadComputer struct {
	podInformer                coreinformers.PodInformer
	resourceReservationManager *ResourceReservationManager
	resourceRequests           ClusterRequests
	nodeLister                 corelisters.NodeLister
	overheadLock               *sync.RWMutex
	ctx                        context.Context
}

// ClusterRequests represents the pod requests in the cluster, indexed by node name
type ClusterRequests map[string]NodeRequests

// NodeRequests represents the currently present pod requests on this node, indexed by pod uid
type NodeRequests map[types.UID]PodRequestInfo

// PodRequestInfo holds information about a pod and its requested resources
type PodRequestInfo struct {
	podName      string
	podNamespace string
	requests     *resources.Resources
}

// NewOverheadComputer creates a new OverheadComputer instance
func NewOverheadComputer(
	ctx context.Context,
	podInformer coreinformers.PodInformer,
	resourceReservationManager *ResourceReservationManager,
	nodeLister corelisters.NodeLister) *OverheadComputer {
	computer := &OverheadComputer{
		podInformer:                podInformer,
		resourceReservationManager: resourceReservationManager,
		resourceRequests:           ClusterRequests{},
		nodeLister:                 nodeLister,
		overheadLock:               &sync.RWMutex{},
		ctx:                        ctx,
	}

	podInformer.Informer().AddEventHandler(
		clientcache.FilteringResourceEventHandler{
			FilterFunc: computer.podHasNodeName,
			Handler: clientcache.ResourceEventHandlerFuncs{
				AddFunc:    computer.addPodRequests,
				DeleteFunc: computer.deletePodRequests,
			},
		})
	return computer
}

func (o *OverheadComputer) getOrCreateNodeRequests(nodeName string) NodeRequests {
	nodeRequests, ok := o.resourceRequests[nodeName]
	if !ok {
		nodeRequests = NodeRequests{}
		o.resourceRequests[nodeName] = nodeRequests
	}
	return nodeRequests
}

// GetOverhead fills overhead information for given nodes.
func (o OverheadComputer) GetOverhead(ctx context.Context, nodes []*v1.Node) resources.NodeGroupResources {
	ov, _ := o.getOverheadByNode(ctx, nodes)
	return ov
}

// GetNonSchedulableOverhead fills non-schedulable overhead information for given nodes.
// Non-schedulable overhead is overhead by pods that are running, but do not have 'spark-scheduler' as their scheduler name.
func (o OverheadComputer) GetNonSchedulableOverhead(ctx context.Context, nodes []*v1.Node) resources.NodeGroupResources {
	_, nso := o.getOverheadByNode(ctx, nodes)
	return nso
}

// getOverheadByNode computes and returns the overhead per node name.
// This returns (overhead per node, nonSchedulableOverhead per node).
func (o OverheadComputer) getOverheadByNode(ctx context.Context, nodes []*v1.Node) (resources.NodeGroupResources, resources.NodeGroupResources) {
	overhead := resources.NodeGroupResources{}
	nonSchedulableOverhead := resources.NodeGroupResources{}

	for _, n := range nodes {
		ov, nso := o.computeNodeOverhead(ctx, n.Name)
		overhead[n.Name] = ov
		nonSchedulableOverhead[n.Name] = nso
	}
	svc1log.FromContext(ctx).Debug("using overhead for nodes", svc1log.SafeParam("overhead", overhead), svc1log.SafeParam("nonSchedulableOverhead", nonSchedulableOverhead))
	return overhead, nonSchedulableOverhead
}

// computeNodeOverhead adds the requests of pods that don't have reservations and are counted as overhead.
// This returns (overhead, nonSchedulableOverhead).
func (o *OverheadComputer) computeNodeOverhead(ctx context.Context, nodeName string) (*resources.Resources, *resources.Resources) {
	o.overheadLock.RLock()
	defer o.overheadLock.RUnlock()
	nodeRequests, ok := o.resourceRequests[nodeName]
	if !ok {
		return resources.Zero(), resources.Zero()
	}
	overhead := resources.Zero()
	nonSchedulableOverhead := resources.Zero()
	for _, podRequestInfo := range nodeRequests {
		pod, err := o.podInformer.Lister().Pods(podRequestInfo.podNamespace).Get(podRequestInfo.podName)
		if err != nil {
			svc1log.FromContext(ctx).Warn("error when checking if pod has a reservation, node overhead calculation might be inaccurate",
				svc1log.SafeParam("nodeName", nodeName),
				svc1log.SafeParam("podName", podRequestInfo.podName),
				svc1log.SafeParam("podNamespace", podRequestInfo.podNamespace))
			continue
		}
		if !o.resourceReservationManager.PodHasReservation(ctx, pod) {
			overhead.Add(podRequestInfo.requests)
			if pod.Spec.SchedulerName == common.SparkSchedulerName {
				if _, appHasResourceReservation := o.resourceReservationManager.GetResourceReservation(pod.Labels[common.SparkAppIDLabel], pod.Namespace); appHasResourceReservation {
					svc1log.FromContext(ctx).Warn("found spark scheduler pod with no reservation but application has a resource reservation",
						svc1log.SafeParam("nodeName", nodeName),
						svc1log.SafeParam("podName", podRequestInfo.podName),
						svc1log.SafeParam("podNamespace", podRequestInfo.podNamespace))
				}
			} else {
				nonSchedulableOverhead.Add(podRequestInfo.requests)
			}
		}
	}
	return overhead, nonSchedulableOverhead
}

func (o *OverheadComputer) podHasNodeName(obj interface{}) bool {
	if pod, ok := utils.GetPodFromObjectOrTombstone(obj); ok {
		return pod.Spec.NodeName != ""
	}
	svc1log.FromContext(o.ctx).Error("failed to parse object as pod", svc1log.UnsafeParam("obj", obj))
	return false
}

func (o *OverheadComputer) addPodRequests(obj interface{}) {
	o.overheadLock.Lock()
	defer o.overheadLock.Unlock()

	pod, ok := obj.(*v1.Pod)
	if !ok {
		svc1log.FromContext(o.ctx).Error("failed to parse object as pod", svc1log.UnsafeParam("obj", obj))
		return
	}
	nodeRequests := o.getOrCreateNodeRequests(pod.Spec.NodeName)
	nodeRequests[pod.UID] = PodRequestInfo{pod.Name, pod.Namespace, podToResources(o.ctx, pod)}
}

func (o *OverheadComputer) deletePodRequests(obj interface{}) {
	o.overheadLock.Lock()
	defer o.overheadLock.Unlock()

	pod, ok := utils.GetPodFromObjectOrTombstone(obj)
	if !ok {
		svc1log.FromContext(o.ctx).Error("failed to parse object as pod", svc1log.UnsafeParam("obj", obj))
		return
	}
	nodeRequests := o.getOrCreateNodeRequests(pod.Spec.NodeName)
	if _, ok := nodeRequests[pod.UID]; !ok {
		return
	}
	delete(nodeRequests, pod.UID)
	if len(nodeRequests) == 0 {
		delete(o.resourceRequests, pod.Spec.NodeName)
	}
}

func podToResources(ctx context.Context, pod *v1.Pod) *resources.Resources {
	res := resources.Zero()
	for _, c := range pod.Spec.Containers {
		resourceRequests := c.Resources.Requests
		res.AddFromResourceList(resourceRequests)
	}

	// The pod requests = max(sum of container requests, any init containers) to match the way kube-scheduler and kubelet compute the requests
	// Unlike those components though, we do not currently support counting pod overheads
	for _, c := range pod.Spec.InitContainers {
		res.SetMaxResource(c.Resources.Requests)
	}

	return res
}
