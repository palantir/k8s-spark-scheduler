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
)

// IsSparkSchedulerPod returns whether the passed object is a spark application pod which has this scheduler in the scheduler spec
func IsSparkSchedulerPod(obj interface{}) bool {
	if pod, ok := obj.(*v1.Pod); ok {
		_, labelFound := pod.Labels[common.SparkRoleLabel]
		if labelFound && pod.Spec.SchedulerName == common.SparkSchedulerName {
			return true
		}
	}
	return false
}


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
