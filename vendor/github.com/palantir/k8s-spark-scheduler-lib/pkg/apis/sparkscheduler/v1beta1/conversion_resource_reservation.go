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

package v1beta1

import (
	"encoding/json"
	"fmt"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta2"
	werror "github.com/palantir/witchcraft-go-error"
	"sigs.k8s.io/controller-runtime/pkg/conversion"
)

// ConvertTo converts from v1beta1 to the storage version v1beta2
// We first try to take values the v1beta1 struct, we then take all remaining values from the annotations.
func (rr *ResourceReservation) ConvertTo(dstRaw conversion.Hub) error {
	dst, ok := dstRaw.(*v1beta2.ResourceReservation)
	if !ok {
		return werror.Error("dst type not as expected",
			werror.SafeParam("expectedType", fmt.Sprintf("%T", v1beta2.ResourceReservation{})),
			werror.SafeParam("actualType", fmt.Sprintf("%T", dstRaw)))
	}

	dst.ObjectMeta = *rr.ObjectMeta.DeepCopy()

	// Remove the reservation annotation metadata as we don't need it in a v1beta2 object.
	delete(dst.ObjectMeta.Annotations, sparkscheduler.ReservationSpecAnnotationKey)

	dst.Status.Pods = make(map[string]string, len(rr.Status.Pods))
	for key, value := range rr.Status.Pods {
		dst.Status.Pods[key] = value
	}

	// Take the common values from the v1beta1 struct
	dst.Spec.Reservations = make(map[string]v1beta2.Reservation, len(rr.Spec.Reservations))
	for key, value := range rr.Spec.Reservations {
		cpu := value.CPU.DeepCopy()
		memory := value.Memory.DeepCopy()
		dst.Spec.Reservations[key] = v1beta2.Reservation{
			Node: value.Node,
			Resources: v1beta2.ResourceList{
				string(v1beta2.ResourceCPU):    &cpu,
				string(v1beta2.ResourceMemory): &memory,
			},
		}
	}

	// Attempt to take any other values from the ReservationSpecAnnotationKey
	if annotationResourceReservationSpecJSON, ok := rr.ObjectMeta.Annotations[sparkscheduler.ReservationSpecAnnotationKey]; ok {
		var annotationResourceReservationSpec v1beta2.ResourceReservationSpec
		err := json.Unmarshal([]byte(annotationResourceReservationSpecJSON), &annotationResourceReservationSpec)
		if err != nil {
			return err
		}
		for key, annotationReservation := range annotationResourceReservationSpec.Reservations {
			if _, ok := dst.Spec.Reservations[key]; ok {
				// Add all resources we could not get from the v1beta1 struct to the resource list, e.g. NvidiaGPU
				for resourceName, quantity := range annotationReservation.Resources {
					if _, ok := dst.Spec.Reservations[key].Resources[resourceName]; !ok {
						quantityCopy := quantity.DeepCopy()
						dst.Spec.Reservations[key].Resources[resourceName] = &quantityCopy
					}
				}

			}
		}
	}

	return nil
}

// ConvertFrom converts from storage version v1beta2 to v1beta1
func (rr *ResourceReservation) ConvertFrom(srcRaw conversion.Hub) error {
	src, ok := srcRaw.(*v1beta2.ResourceReservation)
	if !ok {
		return werror.Error("src type not as expected",
			werror.SafeParam("expectedType", fmt.Sprintf("%T", v1beta2.ResourceReservation{})),
			werror.SafeParam("actualType", fmt.Sprintf("%T", srcRaw)))
	}

	rr.ObjectMeta = *src.ObjectMeta.DeepCopy()

	// Marshal the reservation spec and store it in the annotations, so we don't lose information in round trip conversions
	reservationSpecBytes, err := json.Marshal(src.Spec)
	if err != nil {
		return err
	}
	if rr.ObjectMeta.Annotations == nil {
		rr.ObjectMeta.Annotations = make(map[string]string, 1)
	}
	rr.ObjectMeta.Annotations[sparkscheduler.ReservationSpecAnnotationKey] = string(reservationSpecBytes)
	rr.Status.Pods = make(map[string]string, len(src.Status.Pods))
	for key, value := range src.Status.Pods {
		rr.Status.Pods[key] = value
	}

	rr.Spec.Reservations = make(map[string]Reservation, len(src.Spec.Reservations))
	for key, value := range src.Spec.Reservations {
		cpu := value.Resources.CPU().DeepCopy()
		memory := value.Resources.Memory().DeepCopy()
		rr.Spec.Reservations[key] = Reservation{
			Node:   value.Node,
			CPU:    cpu,
			Memory: memory,
		}
	}
	return nil
}
