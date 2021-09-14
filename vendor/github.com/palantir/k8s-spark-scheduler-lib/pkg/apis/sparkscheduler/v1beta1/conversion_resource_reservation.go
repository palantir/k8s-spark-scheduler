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
	"fmt"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta2"
	werror "github.com/palantir/witchcraft-go-error"
	"sigs.k8s.io/controller-runtime/pkg/conversion"
)

// ConvertTo converts from v1beta1 to the storage version v1beta2
func (rr *ResourceReservation) ConvertTo(dstRaw conversion.Hub) error {
	dst, ok := dstRaw.(*v1beta2.ResourceReservation)
	if !ok {
		return werror.Error("dst type not as expected",
			werror.SafeParam("expectedType", fmt.Sprintf("%T", v1beta2.ResourceReservation{})),
			werror.SafeParam("actualType", fmt.Sprintf("%T", dstRaw)))
	}

	dst.ObjectMeta = rr.ObjectMeta

	dst.Status.Pods = make(map[string]string, len(rr.Status.Pods))
	for key, value := range rr.Status.Pods {
		dst.Status.Pods[key] = value
	}

	dst.Spec.Reservations = make(map[string]v1beta2.Reservation, len(rr.Spec.Reservations))
	for key, value := range rr.Spec.Reservations {
		dst.Spec.Reservations[key] = v1beta2.Reservation{
			Node:      value.Node,
			CPU:       value.CPU,
			Memory:    value.Memory,
			NvidiaGPU: value.NvidiaGPU,
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

	rr.ObjectMeta = src.ObjectMeta

	rr.Status.Pods = make(map[string]string, len(src.Status.Pods))
	for key, value := range src.Status.Pods {
		rr.Status.Pods[key] = value
	}

	rr.Spec.Reservations = make(map[string]Reservation, len(src.Spec.Reservations))
	for key, value := range src.Spec.Reservations {
		rr.Spec.Reservations[key] = Reservation{
			Node:      value.Node,
			CPU:       value.CPU,
			Memory:    value.Memory,
			NvidiaGPU: value.NvidiaGPU,
		}
	}
	return nil
}
