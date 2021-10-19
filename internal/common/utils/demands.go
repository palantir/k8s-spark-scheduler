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
	"strings"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha2"
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
)

// IsSparkSchedulerDemand returns whether the passed object is a demand created by the spark scheduler extender
func IsSparkSchedulerDemand(obj interface{}) bool {
	if demand, ok := obj.(*v1alpha2.Demand); ok {
		_, labelFound := demand.Labels[common.SparkAppIDLabel]
		return labelFound
	}
	return false
}

// OnDemandFulfilled returns a function that calls the wrapped function if the demand object is fulfilled
func OnDemandFulfilled(ctx context.Context, fn func(*v1alpha2.Demand)) func(interface{}, interface{}) {
	return func(oldObj interface{}, newObj interface{}) {
		oldDemand, ok := oldObj.(*v1alpha2.Demand)
		if !ok {
			svc1log.FromContext(ctx).Error("failed to parse oldObj as demand")
			return
		}
		newDemand, ok := newObj.(*v1alpha2.Demand)
		if !ok {
			svc1log.FromContext(ctx).Error("failed to parse newObj as demand")
		}
		if !isDemandFulfilled(oldDemand) && isDemandFulfilled(newDemand) {
			fn(newDemand)
		}
	}

}

func isDemandFulfilled(demand *v1alpha2.Demand) bool {
	return demand.Status.Phase == v1alpha2.DemandPhaseFulfilled
}

// DemandName returns a demand name from a pod name
func DemandName(pod *v1.Pod) string {
	return "demand-" + pod.Name
}

// PodName returns a pod name from a demand name
func PodName(demand *v1alpha2.Demand) string {
	return strings.TrimPrefix(demand.Name, "demand-")
}
