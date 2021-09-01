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

package logging

import (
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta2"
)

// RRSafeParam gets the safe params for a list of resource reservations
func RRSafeParam(resourceReservation *v1beta2.ResourceReservation) map[string]interface{} {
	names := make([]string, 0, len(resourceReservation.Spec.Reservations))
	pods := make([]string, 0, len(resourceReservation.Spec.Reservations))
	nodes := make([]string, 0, len(resourceReservation.Spec.Reservations))
	for name, reservation := range resourceReservation.Spec.Reservations {
		names = append(names, name)
		pods = append(pods, resourceReservation.Status.Pods[name])
		nodes = append(nodes, reservation.Node)
	}
	return map[string]interface{}{
		"resourceReservationNames": names,
		"resourceReservationPods":  pods,
		"resourceReservationNodes": nodes,
	}
}
