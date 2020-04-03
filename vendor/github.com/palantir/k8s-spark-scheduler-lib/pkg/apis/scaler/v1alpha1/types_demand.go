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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Demand represents currently unschedulable resources.
type Demand struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DemandSpec   `json:"spec,omitempty"`
	Status DemandStatus `json:"status,omitempty"`
}

// DemandSpec represents the units and status of a Demand resource
type DemandSpec struct {
	Units         []DemandUnit `json:"units"`
	InstanceGroup string       `json:"instance-group"`

	// IsLongLived changes the lifecycle for a demand from
	// ephemeral and immutable to long-lived and mutable.
	// This is useful to set a buffer in an instance-group:
	// an amount of compute resources that is left unused
	// but ready for quick reservation should there be need.
	IsLongLived bool `json:"is-long-lived"`
}

// DemandStatus represents the status a demand object is in
type DemandStatus struct {
	Phase string `json:"phase"`
}

// DemandUnit represents a single unit of demand as a count of CPU and Memory requirements
type DemandUnit struct {
	CPU    resource.Quantity `json:"cpu"`
	Memory resource.Quantity `json:"memory"`
	Count  int               `json:"count"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DemandList is a list of Demand resources
type DemandList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Demand `json:"items"`
}
