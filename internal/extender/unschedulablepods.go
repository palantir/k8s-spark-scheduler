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
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/predicates"
)

const (
	podExceedsClusterCapacity       v1.PodConditionType = "PodExceedsClusterCapacity"
	unschedulablePollingInterval    time.Duration       = time.Minute
	unschedulableInClusterThreshold time.Duration       = 10 * time.Minute
)

// UnschedulablePodMarker checks for spark scheduler managed pending driver pods
// and checks if they can fit if the cluster was empty, else marks them with a
// custom pod condition.
type UnschedulablePodMarker struct {
	nodeLister       corelisters.NodeLister
	podLister        corelisters.PodLister
	coreClient       corev1.CoreV1Interface
	overheadComputer *OverheadComputer
	binpacker        *Binpacker
}

// NewUnschedulablePodMarker creates a new UnschedulablePodMarker
func NewUnschedulablePodMarker(
	nodeLister corelisters.NodeLister,
	podLister corelisters.PodLister,
	coreClient corev1.CoreV1Interface,
	overheadComputer *OverheadComputer,
	binpacker *Binpacker) *UnschedulablePodMarker {
	return &UnschedulablePodMarker{
		nodeLister:       nodeLister,
		podLister:        podLister,
		coreClient:       coreClient,
		overheadComputer: overheadComputer,
		binpacker:        binpacker,
	}
}

// Start starts periodic scanning for unschedulable applications
func (u *UnschedulablePodMarker) Start(ctx context.Context) {
	_ = wapp.RunWithFatalLogging(ctx, u.doStart)
}

func (u *UnschedulablePodMarker) doStart(ctx context.Context) error {
	t := time.NewTicker(unschedulablePollingInterval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			u.scanForUnschedulablePods(ctx)
		}
	}
}

func (u *UnschedulablePodMarker) scanForUnschedulablePods(ctx context.Context) {
	pods, err := u.podLister.List(labels.Everything())
	if err != nil {
		svc1log.FromContext(ctx).Error("failed to list pods", svc1log.Stacktrace(err))
		return
	}
	now := time.Now()
	for _, pod := range pods {
		if pod.Spec.SchedulerName == SparkSchedulerName &&
			len(pod.Spec.NodeName) == 0 &&
			pod.DeletionTimestamp == nil &&
			pod.Labels[SparkRoleLabel] == Driver &&
			pod.CreationTimestamp.Time.Add(unschedulableInClusterThreshold).Before(now) {

			ctx = svc1log.WithLoggerParams(
				ctx,
				svc1log.SafeParam("podName", pod.Name),
				svc1log.SafeParam("podNamespace", pod.Namespace))

			exceedsCapacity, err := u.DoesPodExceedClusterCapacity(ctx, pod)
			if err != nil {
				svc1log.FromContext(ctx).Error("failed to check if pod was unschedulable",
					svc1log.Stacktrace(err))
				return
			}
			if exceedsCapacity {
				svc1log.FromContext(ctx).Info("Marking pod as exceeds capacity")
			}
			err = u.markPodClusterCapacityStatus(ctx, pod, exceedsCapacity)
			if err != nil {
				svc1log.FromContext(ctx).Error("failed to mark pod cluster capacity status",
					svc1log.Stacktrace(err))
			}
		}
	}
}

// DoesPodExceedClusterCapacity checks if the provided driver pod could ever fit to the cluster
func (u *UnschedulablePodMarker) DoesPodExceedClusterCapacity(ctx context.Context, driver *v1.Pod) (bool, error) {
	nodes, err := u.nodeLister.ListWithPredicate(func(node *v1.Node) bool {
		return predicates.PodMatchesNodeSelectorAndAffinityTerms(driver, node)
	})
	if err != nil {
		return false, err
	}
	nodeNames := make([]string, 0, len(nodes))
	for _, node := range nodes {
		nodeNames = append(nodeNames, node.Name)
	}
	if len(nodeNames) == 0 {
		svc1log.FromContext(ctx).Info("could not find any nodes matching pod selectors",
			svc1log.SafeParam("nodeSelector", driver.Spec.NodeSelector))
	}

	availableNodesSchedulingMetadata := resources.NodeSchedulingMetadataForNodes(nodes, u.overheadComputer.GetNonSchedulableOverhead(ctx, nodes))
	applicationResources, err := sparkResources(ctx, driver)
	if err != nil {
		return false, err
	}
	_, _, hasCapacity := u.binpacker.BinpackFunc(
		ctx,
		applicationResources.driverResources,
		applicationResources.executorResources,
		applicationResources.minExecutorCount,
		nodeNames,
		nodeNames,
		availableNodesSchedulingMetadata)

	return !hasCapacity, nil
}

func (u *UnschedulablePodMarker) markPodClusterCapacityStatus(ctx context.Context, driver *v1.Pod, exceedsCapacity bool) error {
	exceedsCapacityStatus := v1.ConditionFalse
	if exceedsCapacity {
		exceedsCapacityStatus = v1.ConditionTrue
	}

	if !podutil.UpdatePodCondition(&driver.Status, &v1.PodCondition{Type: podExceedsClusterCapacity, Status: exceedsCapacityStatus}) {
		return nil
	}

	_, err := u.coreClient.Pods(driver.Namespace).UpdateStatus(driver)
	return err
}
