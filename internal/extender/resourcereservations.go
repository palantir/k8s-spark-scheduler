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
	"fmt"
	"github.com/palantir/k8s-spark-scheduler/internal/common"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var podGroupVersionKind = v1.SchemeGroupVersion.WithKind("Pod")

func newResourceReservation(driverNode string, executorNodes []string, driver *v1.Pod, driverResources, executorResources *resources.Resources) *v1beta1.ResourceReservation {
	reservations := make(map[string]v1beta1.Reservation, len(executorNodes)+1)
	reservations["driver"] = v1beta1.Reservation{
		Node:   driverNode,
		CPU:    driverResources.CPU,
		Memory: driverResources.Memory,
	}
	for idx, nodeName := range executorNodes {
		reservations[executorReservationName(idx)] = v1beta1.Reservation{
			Node:   nodeName,
			CPU:    executorResources.CPU,
			Memory: executorResources.Memory,
		}
	}
	return &v1beta1.ResourceReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name:            driver.Labels[common.SparkAppIDLabel],
			Namespace:       driver.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(driver, podGroupVersionKind)},
			Labels: map[string]string{
				v1beta1.AppIDLabel: driver.Labels[common.SparkAppIDLabel],
			},
		},
		Spec: v1beta1.ResourceReservationSpec{
			Reservations: reservations,
		},
		Status: v1beta1.ResourceReservationStatus{
			Pods: map[string]string{"driver": driver.Name},
		},
	}
}

func executorReservationName(i int) string {
	return fmt.Sprintf("executor-%d", i+1)
}
