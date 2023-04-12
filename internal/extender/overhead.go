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

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	corelisters "k8s.io/client-go/listers/core/v1"
)

// OverheadComputer computes non spark scheduler managed pods total resources periodically
type defaultOverheadComputer struct {
	podLister                  corelisters.PodLister
	resourceReservationManager ResourceReservationManager
}

// NodeRequests represents the currently present pod requests on this node, indexed by pod uid
type NodeRequests map[types.UID]PodRequestInfo

// PodRequestInfo holds information about a pod and its requested resources
type PodRequestInfo struct {
	podName      string
	podNamespace string
	requests     *resources.Resources
}

// OverheadComputer exposes methods for determining how many resources have alread
type OverheadComputer interface {
	GetOverhead(ctx context.Context, nodes []*v1.Node) (resources.NodeGroupResources, error)
	GetNonSchedulableOverhead(ctx context.Context, nodes []*v1.Node) (resources.NodeGroupResources, error)
}

// NewOverheadComputer creates a new OverheadComputer instance
func NewOverheadComputer(
	resourceReservationManager ResourceReservationManager,
	podLister corelisters.PodLister) OverheadComputer {
	return &defaultOverheadComputer{
		resourceReservationManager: resourceReservationManager,
		podLister:                  podLister,
	}
}

// GetOverhead fills overhead information for given nodes.
func (o *defaultOverheadComputer) GetOverhead(ctx context.Context, nodes []*v1.Node) (resources.NodeGroupResources, error) {
	ov, _, err := o.getOverheadByNode(ctx, nodes)
	if err != nil {
		return resources.NodeGroupResources{}, err
	}
	return ov, nil
}

// GetNonSchedulableOverhead fills non-schedulable overhead information for given nodes.
// Non-schedulable overhead is overhead by pods that are running, but do not have 'spark-scheduler' as their scheduler name.
func (o *defaultOverheadComputer) GetNonSchedulableOverhead(ctx context.Context, nodes []*v1.Node) (resources.NodeGroupResources, error) {
	_, nso, err := o.getOverheadByNode(ctx, nodes)
	if err != nil {
		return resources.NodeGroupResources{}, err
	}
	return nso, nil
}

// getOverheadByNode computes and returns the overhead per node name.
// This returns (overhead per node, nonSchedulableOverhead per node).
func (o *defaultOverheadComputer) getOverheadByNode(ctx context.Context, nodes []*v1.Node) (resources.NodeGroupResources, resources.NodeGroupResources, error) {
	overhead := resources.NodeGroupResources{}
	nonSchedulableOverhead := resources.NodeGroupResources{}

	for _, n := range nodes {
		ov, nso, err := o.computeNodeOverhead(ctx, n.Name)
		if err != nil {
			return resources.NodeGroupResources{}, resources.NodeGroupResources{}, err
		}
		overhead[n.Name] = ov
		nonSchedulableOverhead[n.Name] = nso
	}
	svc1log.FromContext(ctx).Debug("using overhead for nodes", svc1log.SafeParam("overhead", overhead), svc1log.SafeParam("nonSchedulableOverhead", nonSchedulableOverhead))
	return overhead, nonSchedulableOverhead, nil
}

// computeNodeOverhead adds the requests of pods that don't have reservations and are counted as overhead.
// This returns (overhead, nonSchedulableOverhead).
func (o *defaultOverheadComputer) computeNodeOverhead(ctx context.Context, nodeName string) (*resources.Resources, *resources.Resources, error) {
	podsOnNode, err := o.getPodsOnNode(ctx, nodeName)
	if err != nil {
		return nil, nil, err
	}
	overhead := resources.Zero()
	nonSchedulableOverhead := resources.Zero()
	for _, p := range podsOnNode {
		pod := p
		if o.resourceReservationManager.PodHasReservation(ctx, pod) {
			continue
		}
		requests := o.podToResources(pod)
		overhead.Add(requests)
		if pod.Spec.SchedulerName == common.SparkSchedulerName {
			if _, appHasResourceReservation := o.resourceReservationManager.GetResourceReservation(pod.Labels[common.SparkAppIDLabel], pod.Namespace); appHasResourceReservation {
				svc1log.FromContext(ctx).Warn("found spark scheduler pod with no reservation but application has a resource reservation",
					svc1log.SafeParam("nodeName", nodeName),
					svc1log.SafeParam("podName", pod.Name),
					svc1log.SafeParam("podNamespace", pod.Namespace))
			}
		} else {
			nonSchedulableOverhead.Add(requests)
		}
	}
	return overhead, nonSchedulableOverhead, nil
}

func (o *defaultOverheadComputer) getPodsOnNode(ctx context.Context, nodeName string) ([]*v1.Pod, error) {
	pods, err := o.podLister.List(labels.Everything())
	if err != nil {
		return nil, werror.WrapWithContextParams(ctx, err, "could not list from pords")
	}
	var podsOnNode []*v1.Pod
	for _, p := range pods {
		pod := p
		if pod.Spec.NodeName != nodeName {
			continue
		}
		podsOnNode = append(podsOnNode, pod)

	}
	return podsOnNode, nil
}

func (o *defaultOverheadComputer) podToResources(pod *v1.Pod) *resources.Resources {
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
