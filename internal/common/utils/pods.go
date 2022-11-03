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

package utils

import (
	"context"

	"github.com/palantir/k8s-spark-scheduler/internal/common"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
	clientcache "k8s.io/client-go/tools/cache"
)

// IsSparkSchedulerPod returns whether the passed object is a spark application pod which has this scheduler in the scheduler spec
func IsSparkSchedulerPod(obj interface{}) bool {
	_, isSparkSchedulerPod := getRoleIfSparkSchedulerPod(obj)
	return isSparkSchedulerPod
}

// IsSparkSchedulerExecutorPod returns whether the passed object is a spark application pod which has this scheduler in the scheduler spec and is an executor
func IsSparkSchedulerExecutorPod(obj interface{}) bool {
	if role, isSparkSchedulerPod := getRoleIfSparkSchedulerPod(obj); isSparkSchedulerPod {
		return role == common.Executor
	}
	return false
}

// GetPodFromObjectOrTombstone tries to cast the passed object to a Pod object, and if that's not possible tries to get the Pod object from the tombstone
func GetPodFromObjectOrTombstone(obj interface{}) (*v1.Pod, bool) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		tombstone, ok := obj.(clientcache.DeletedFinalStateUnknown)
		if !ok {
			return nil, false
		}
		pod, ok = tombstone.Obj.(*v1.Pod)
		if !ok {
			return nil, false
		}
	}
	return pod, true
}

func getRoleIfSparkSchedulerPod(obj interface{}) (string, bool) {
	if pod, ok := obj.(*v1.Pod); ok {
		role, labelFound := pod.Labels[common.SparkRoleLabel]
		if labelFound && pod.Spec.SchedulerName == common.SparkSchedulerName {
			return role, true
		}
	}
	return "", false
}

// IsPodTerminated returns whether the pod is considered to be terminated
func IsPodTerminated(pod *v1.Pod) bool {
	allTerminated := len(pod.Status.ContainerStatuses) > 0
	for _, status := range pod.Status.ContainerStatuses {
		allTerminated = allTerminated && status.State.Terminated != nil
	}
	return allTerminated
}

// OnPodScheduled returns a function that calls the wrapped function if the pod is scheduled
func OnPodScheduled(ctx context.Context, fn func(*v1.Pod)) func(interface{}, interface{}) {
	return func(oldObj interface{}, newObj interface{}) {
		oldPod, ok := oldObj.(*v1.Pod)
		if !ok {
			svc1log.FromContext(ctx).Error("failed to parse oldObj as pod")
			return
		}
		newPod, ok := newObj.(*v1.Pod)
		if !ok {
			svc1log.FromContext(ctx).Error("failed to parse newObj as pod")
			return
		}
		if !isPodScheduled(oldPod) && isPodScheduled(newPod) {
			fn(newPod)
		}
	}
}

func isPodScheduled(pod *v1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == v1.PodScheduled && cond.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

// NodeConditionPredicate is a function that indicates whether the given node's conditions meet
// some set of criteria defined by the function.
type NodeConditionPredicate func(node *v1.Node) (bool, error)

// ListWithPredicate gets nodes that matches predicate function.
func ListWithPredicate(nodeLister corelisters.NodeLister, predicate NodeConditionPredicate) ([]*v1.Node, error) {
	nodes, err := nodeLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var filtered []*v1.Node
	for i := range nodes {
		matches, err := predicate(nodes[i])
		if err != nil {
			return nil, err
		}
		if matches {
			filtered = append(filtered, nodes[i])
		}
	}

	return filtered, nil
}
