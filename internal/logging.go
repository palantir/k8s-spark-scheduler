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

package internal

import (
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha1"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	v1 "k8s.io/api/core/v1"
)

// DemandSafeParamsFromObj gets the safe params for a demand object
func DemandSafeParamsFromObj(demand *v1alpha1.Demand) map[string]interface{} {
	return map[string]interface{}{
		"demandName":      demand.Name,
		"demandNamespace": demand.Namespace,
	}
}

// DemandSafeParams gets the safe params for a demand object
func DemandSafeParams(demandName string, demandNamespace string) map[string]interface{} {
	return map[string]interface{}{
		"demandName":      demandName,
		"demandNamespace": demandNamespace,
	}
}

// ResourceReservationSafeParamsFromObj gets the safe params for a resource reservation object
func ResourceReservationSafeParamsFromObj(rr *v1beta1.ResourceReservation) map[string]interface{} {
	return map[string]interface{}{
		"resourceReservationName":      rr.Name,
		"resourceReservationNamespace": rr.Namespace,
	}
}

// PodSafeParams gets the safe params for a driver pod
func PodSafeParams(pod v1.Pod) map[string]interface{} {
	return map[string]interface{}{
		"podName":      pod.Name,
		"podNamespace": pod.Namespace,
	}
}
