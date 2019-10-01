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
	"sync"
	"time"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
)

var (
	oneCPU = resource.NewMilliQuantity(1000, resource.DecimalSI)
	oneGiB = resource.NewQuantity(1*1024*1024*1024, resource.BinarySI)
)

// OverheadComputer computes non spark scheduler managed pods total resources periodically
type OverheadComputer struct {
	podLister            corelisters.PodLister
	resourceReservations *cache.ResourceReservationCache
	nodeLister           corelisters.NodeLister
	latestOverhead       Overhead
	overheadLock         *sync.RWMutex
	instanceGroupLabel   string
}

// Overhead represents the overall overhead in the cluster, indexed by instance groups
type Overhead map[string]*InstanceGroupOverhead

// InstanceGroupOverhead keeps overhead for a group of nodes, and the median overhead
type InstanceGroupOverhead struct {
	overhead       resources.NodeGroupResources
	medianOverhead *resources.Resources
}

// NewOverheadComputer creates a new OverheadComputer instance
func NewOverheadComputer(
	ctx context.Context,
	podLister corelisters.PodLister,
	resourceReservations *cache.ResourceReservationCache,
	nodeLister corelisters.NodeLister,
	instanceGroupLabel string) *OverheadComputer {
	computer := &OverheadComputer{
		podLister:            podLister,
		resourceReservations: resourceReservations,
		nodeLister:           nodeLister,
		overheadLock:         &sync.RWMutex{},
		instanceGroupLabel:   instanceGroupLabel,
	}
	computer.compute(ctx)
	return computer
}

// Start starts periodic scanning for overhead
func (o *OverheadComputer) Start(ctx context.Context) {
	_ = wapp.RunWithFatalLogging(ctx, o.doStart)
}

func (o *OverheadComputer) doStart(ctx context.Context) error {
	t := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			o.compute(ctx)
		}
	}
}

func (o *OverheadComputer) compute(ctx context.Context) {
	pods, err := o.podLister.List(labels.Everything())
	if err != nil {
		svc1log.FromContext(ctx).Error("failed to list pods", svc1log.Stacktrace(err))
		return
	}
	rrs := o.resourceReservations.List()
	podsWithRRs := make(map[string]bool, len(rrs))
	for _, rr := range rrs {
		for _, podName := range rr.Status.Pods {
			podsWithRRs[podName] = true
		}
	}
	rawOverhead := map[string]resources.NodeGroupResources{}
	for _, p := range pods {
		if podsWithRRs[p.Name] {
			continue
		}
		if p.Spec.NodeName == "" || p.Status.Phase == v1.PodSucceeded || p.Status.Phase == v1.PodFailed {
			// pending pod or pod succeeded or failed
			continue
		}
		node, err := o.nodeLister.Get(p.Spec.NodeName)
		if err != nil {
			svc1log.FromContext(ctx).Warn("node does not exist in cache", svc1log.SafeParam("nodeName", p.Spec.NodeName))
			continue
		}
		// found pod with not associated resource reservation, add to overhead
		instanceGroup := node.Labels[o.instanceGroupLabel]
		if _, ok := rawOverhead[instanceGroup]; !ok {
			rawOverhead[instanceGroup] = resources.NodeGroupResources{}
		}
		currentOverhead := rawOverhead[instanceGroup]
		if _, ok := currentOverhead[p.Spec.NodeName]; !ok {
			currentOverhead[p.Spec.NodeName] = resources.Zero()
		}
		currentOverhead[p.Spec.NodeName].Add(podToResources(ctx, p))
	}
	overhead := Overhead{}
	for instanceGroup, nodeGroupResources := range rawOverhead {
		resourcesSlice := make([]*resources.Resources, 0, len(nodeGroupResources))
		for _, resources := range nodeGroupResources {
			resourcesSlice = append(resourcesSlice, resources)
		}
		sort.Slice(resourcesSlice, func(i, j int) bool {
			return resourcesSlice[i].GreaterThan(resourcesSlice[j])
		})
		medianOverhead := resourcesSlice[len(resourcesSlice)/2]
		svc1log.FromContext(ctx).Info("computed overhead",
			svc1log.SafeParam("medianOverhead", medianOverhead),
			svc1log.SafeParam("instanceGroup", instanceGroup))

		overhead[instanceGroup] = &InstanceGroupOverhead{
			rawOverhead[instanceGroup],
			medianOverhead,
		}
	}
	o.overheadLock.Lock()
	o.latestOverhead = overhead
	o.overheadLock.Unlock()
}

func podToResources(ctx context.Context, pod *v1.Pod) *resources.Resources {
	res := resources.Zero()
	for _, c := range pod.Spec.Containers {
		resourceRequests := c.Resources.Requests
		if resourceRequests.Cpu().AsDec().Cmp(oneCPU.AsDec()) > 0 || resourceRequests.Memory().AsDec().Cmp(oneGiB.AsDec()) > 0 {
			svc1log.FromContext(ctx).Debug("Container with no resource reservation has high resource requests",
				svc1log.SafeParam("podName", pod.Name),
				svc1log.SafeParam("nodeName", pod.Spec.NodeName),
				svc1log.SafeParam("CPU", resourceRequests.Cpu()),
				svc1log.SafeParam("Memory", resourceRequests.Memory()))
		}
		res.AddFromResourceList(resourceRequests)
	}
	return res
}

// GetOverhead fills overhead information for given nodes, and falls back to the median overhead
// of the instance group if the node is not found
func (o OverheadComputer) GetOverhead(ctx context.Context, nodes []*v1.Node) resources.NodeGroupResources {
	o.overheadLock.RLock()
	defer o.overheadLock.RUnlock()
	res := resources.NodeGroupResources{}
	if o.latestOverhead == nil {
		return res
	}
	for _, n := range nodes {
		instanceGroup := n.Labels[o.instanceGroupLabel]
		instanceGroupOverhead := o.latestOverhead[instanceGroup]
		if instanceGroupOverhead == nil {
			svc1log.FromContext(ctx).Warn("overhead for instance group does not exist", svc1log.SafeParam("instanceGroup", instanceGroup))
			continue
		}
		if nodeOverhead, ok := instanceGroupOverhead.overhead[n.Name]; ok {
			res[n.Name] = nodeOverhead
		} else {
			res[n.Name] = instanceGroupOverhead.medianOverhead
		}
	}
	svc1log.FromContext(ctx).Debug("using overhead for nodes", svc1log.SafeParam("overhead", res))
	return res
}
