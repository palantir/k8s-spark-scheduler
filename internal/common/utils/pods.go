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
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	v1 "k8s.io/api/core/v1"
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
	if pod, ok := GetPodFromObjectOrTombstone(obj); ok {
		role, labelFound := pod.Labels[common.SparkRoleLabel]
		if labelFound && pod.Spec.SchedulerName == common.SparkSchedulerName {
			return role, true
		}
	}
	return "", false
}
