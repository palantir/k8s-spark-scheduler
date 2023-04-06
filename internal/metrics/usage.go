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

package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta2"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	corelisters "k8s.io/client-go/listers/core/v1"
)

// ResourceUsageReporter reports resource usage periodically
type ResourceUsageReporter struct {
	nodeLister            corelisters.NodeLister
	podLister             corelisters.PodLister
	resourceReservations  *cache.ResourceReservationCache
	instanceGroupTagLabel string
}

// NewResourceReporter returns a new ResourceUsageReporter instance
func NewResourceReporter(
	nodeLister corelisters.NodeLister,
	podLister corelisters.PodLister,
	resourceReservations *cache.ResourceReservationCache,
	instanceGroupTagLabel string) *ResourceUsageReporter {
	return &ResourceUsageReporter{
		nodeLister:            nodeLister,
		podLister:             podLister,
		resourceReservations:  resourceReservations,
		instanceGroupTagLabel: instanceGroupTagLabel,
	}
}

// StartReportingResourceUsage starts periodic resource usage reporting
func (r *ResourceUsageReporter) StartReportingResourceUsage(ctx context.Context) {
	_ = wapp.RunWithFatalLogging(ctx, r.doStart)
}

func (r *ResourceUsageReporter) doStart(ctx context.Context) error {
	t := time.NewTicker(30 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			req, err := labels.NewRequirement(r.instanceGroupTagLabel, selection.Exists, []string{})
			if err != nil {
				svc1log.FromContext(ctx).Error("failed to create requirement for instance group label exists", svc1log.Stacktrace(err))
				break
			}
			hasInstanceGroup := labels.NewSelector().Add(*req)
			nodes, err := r.nodeLister.List(hasInstanceGroup)
			if err != nil {
				svc1log.FromContext(ctx).Error("failed to list resource reservations", svc1log.Stacktrace(err))
				break
			}
			nodeNames := make([]string, 0, len(nodes))
			for _, n := range nodes {
				nodeNames = append(nodeNames, n.Name)
			}
			rrs := r.resourceReservations.List()
			r.report(ctx, nodes, rrs)
		}
	}
}

func (r *ResourceUsageReporter) report(ctx context.Context, nodes []*v1.Node, rrs []*v1beta2.ResourceReservation) {
	resourceUsages := resources.UsageForNodes(rrs)

	tagsToDelete := make([]metrics.Tags, 0, len(resourceUsages))
	metrics.FromContext(ctx).Each(func(name string, tags metrics.Tags, value metrics.MetricVal) {
		host, hostTagExists := tags.ToMap()[hostTagName]
		if !hostTagExists {
			return
		}
		if _, ok := resourceUsages[host]; !ok {
			tagsToDelete = append(tagsToDelete, tags)
		}
	})
	for _, tags := range tagsToDelete {
		metrics.FromContext(ctx).Unregister(resourceUsageCPU, tags...)
		metrics.FromContext(ctx).Unregister(resourceUsageMemory, tags...)
		metrics.FromContext(ctx).Unregister(resourceUsageNvidiaGPUs, tags...)
	}
	pods, err := r.podLister.List(labels.Everything())
	if err != nil {
		return
	}

	for _, node := range nodes {
		n := node

		// And a log statement
		resourcesOnNode := resources.Zero()
		podSeen := map[string]struct{}{}
		for _, podInLister := range pods {
			pod := podInLister
			if pod.Spec.NodeName != n.Name {
				continue
			}
			resourcesOnNode.Add(forPod(*pod))
			podSeen[pod.Namespace+pod.Name] = struct{}{}
		}

		for _, resourceReservation := range rrs {
			for podId, reservation := range resourceReservation.Spec.Reservations {
				if reservation.Node != n.Name {
					continue
				}
				podName := resourceReservation.Status.Pods[podId]
				keyName := resourceReservation.Namespace + podName
				_, ok := podSeen[keyName]
				if ok {
					continue
				}
				resourcesOnNode.AddFromReservation(&reservation)
			}
		}
		zone := n.Labels[v1.LabelZoneFailureDomain]
		instanceGroup := n.Labels["com.palantir.rubix/instance-group"]
		fmt.Println("hi")
		svc1log.FromContext(ctx).Info("Basic quantity check of node", svc1log.SafeParams(map[string]interface{}{
			"nodeName":      n.Name,
			"zone":          zone,
			"instanceGroup": instanceGroup,
			"CPU":           resourcesOnNode.CPU.String(),
			"Memory":        resourcesOnNode.Memory.String(),
			"NvidiaGPU":     resourcesOnNode.NvidiaGPU.String(),
		}))

		usage, ok := resourceUsages[n.Name]
		if !ok {
			continue
		}
		hostTag := HostTag(ctx, n.Name)
		instanceGroupTag := InstanceGroupTag(ctx, n.Labels[r.instanceGroupTagLabel])
		metrics.FromContext(ctx).Gauge(resourceUsageCPU, hostTag, instanceGroupTag).Update(usage.CPU.Value())
		metrics.FromContext(ctx).Gauge(resourceUsageMemory, hostTag, instanceGroupTag).Update(usage.Memory.Value())
		metrics.FromContext(ctx).Gauge(resourceUsageNvidiaGPUs, hostTag, instanceGroupTag).Update(usage.NvidiaGPU.Value())

	}
	fmt.Println("a")
}

func forPod(pod v1.Pod) *resources.Resources {
	onPod := resources.Zero()
	for _, container := range pod.Spec.Containers {
		onPod.AddFromResourceList(container.Resources.Requests)
	}
	return onPod
}
