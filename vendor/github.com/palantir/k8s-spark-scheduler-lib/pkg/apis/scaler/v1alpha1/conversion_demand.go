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

package v1alpha1

import (
	"fmt"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha2"
	werror "github.com/palantir/witchcraft-go-error"
	"sigs.k8s.io/controller-runtime/pkg/conversion"
)

// ConvertTo converts from v1alpha1 to the storage version v1alpha2
func (d *Demand) ConvertTo(dstRaw conversion.Hub) error {
	dst, ok := dstRaw.(*v1alpha2.Demand)
	if !ok {
		return werror.Error("dst type not as expected",
			werror.SafeParam("expectedType", fmt.Sprintf("%T", v1alpha2.Demand{})),
			werror.SafeParam("actualType", fmt.Sprintf("%T", dstRaw)))
	}

	dst.ObjectMeta = d.ObjectMeta

	dst.Status.LastTransitionTime = d.Status.LastTransitionTime
	dst.Status.Phase = v1alpha2.DemandPhase(d.Status.Phase)

	dst.Spec.InstanceGroup = d.Spec.InstanceGroup
	dst.Spec.IsLongLived = d.Spec.IsLongLived

	dstUnits := make([]v1alpha2.DemandUnit, 0, len(d.Spec.Units))
	for _, u := range d.Spec.Units {
		dstUnits = append(dstUnits, v1alpha2.DemandUnit{
			Resources: v1alpha2.ResourceList{
				v1alpha2.ResourceCPU:       u.CPU,
				v1alpha2.ResourceMemory:    u.Memory,
				v1alpha2.ResourceNvidiaGPU: u.GPU,
			},
			Count: u.Count,
		})
	}
	dst.Spec.Units = dstUnits

	return nil
}

// ConvertFrom converts from storage version v1alpha2 to v1alpha1
func (d *Demand) ConvertFrom(srcRaw conversion.Hub) error {
	src, ok := srcRaw.(*v1alpha2.Demand)
	if !ok {
		return werror.Error("src type not as expected",
			werror.SafeParam("expectedType", fmt.Sprintf("%T", v1alpha2.Demand{})),
			werror.SafeParam("actualType", fmt.Sprintf("%T", srcRaw)))
	}

	d.ObjectMeta = src.ObjectMeta

	d.Status.LastTransitionTime = src.Status.LastTransitionTime
	d.Status.Phase = string(src.Status.Phase)

	d.Spec.InstanceGroup = src.Spec.InstanceGroup
	d.Spec.IsLongLived = src.Spec.IsLongLived

	dstUnits := make([]DemandUnit, 0, len(src.Spec.Units))
	for _, u := range src.Spec.Units {
		demandUnit := DemandUnit{
			Count: u.Count,
		}
		for name, quantity := range u.Resources {
			switch name {
			case v1alpha2.ResourceCPU:
				demandUnit.CPU = quantity
			case v1alpha2.ResourceMemory:
				demandUnit.Memory = quantity
			case v1alpha2.ResourceNvidiaGPU:
				demandUnit.GPU = quantity
			default:
				return werror.Error("unsupported resource found during demand conversion from storage version to v1alpha1",
					werror.SafeParam("resourceName", name),
					werror.SafeParam("resourceQuantity", quantity))
			}
		}
		dstUnits = append(dstUnits, demandUnit)
	}
	d.Spec.Units = dstUnits

	return nil
}
