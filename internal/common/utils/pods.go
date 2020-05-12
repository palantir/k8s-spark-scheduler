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

// IsPodTerminated returns whether the pod is considered to be terminated
func IsPodTerminated(pod *v1.Pod) bool {
	allTerminated := len(pod.Status.ContainerStatuses) > 0
	for _, status := range pod.Status.ContainerStatuses {
		allTerminated = allTerminated && status.State.Terminated != nil
	}
	return allTerminated
}
