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

package v1alpha2

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DemandPhase type declares that the value is a Demands phase
type DemandPhase string

const (
	// DemandPhaseEmpty is the state of a demand object when it is first created
	DemandPhaseEmpty DemandPhase = ""
	// DemandPhasePending is the state a demand object is in when Scaler has acknowledged it but has not yet taken
	// action to fulfill the demand
	DemandPhasePending DemandPhase = "pending"
	// DemandPhaseFulfilled is the state a demand object is in when Scaler has taken action and the action has completed
	// to fulfill the demand. At this point, it is expected that there is capacity to meet the demand the object represents
	DemandPhaseFulfilled DemandPhase = "fulfilled"
	// DemandPhaseCannotFulfill is the state a demand object is in when Scaler is unable to satisfy the demand. This is
	// possible if the demand contains a single unit that is larger than the instance group is configured to use, or if
	// the instance group has reached its maximum capacity and cannot allocate more
	DemandPhaseCannotFulfill DemandPhase = "cannot-fulfill"

	// ResourceCPU is the name of CPU resource.
	ResourceCPU corev1.ResourceName = corev1.ResourceCPU
	// ResourceMemory is the name of Memory resource.
	ResourceMemory corev1.ResourceName = corev1.ResourceMemory
	// ResourceNvidiaGPU is the name of Nvidia GPU resource.
	ResourceNvidiaGPU corev1.ResourceName = "nvidia.com/gpu"
)

var (
	// AllDemandPhases is a list of all phases that a demand object could be in
	AllDemandPhases = []DemandPhase{
		DemandPhaseEmpty,
		DemandPhasePending,
		DemandPhaseFulfilled,
		DemandPhaseCannotFulfill,
	}

	// AllSupportedResources is a list of all resources that the demand object supports
	AllSupportedResources = []corev1.ResourceName{
		ResourceCPU,
		ResourceMemory,
		ResourceNvidiaGPU,
	}
)

// +genclient
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
	// EnforceSingleZoneScheduling indicates this demand must
	// be satisfied in a single zone.
	EnforceSingleZoneScheduling bool `json:"enforce-single-zone-scheduling"`
}

// DemandStatus represents the status a demand object is in
type DemandStatus struct {
	// Phase denotes the demand phase.
	Phase DemandPhase `json:"phase"`
	// LastTransitionTime denotes the last transition time of the demand phase.
	// If left empty, defaults to the creation time of the demand.
	// +optional
	LastTransitionTime metav1.Time `json:"last-transition-time,omitempty"`
	// FulfilledZone is the zone that was scaled up to satisfy this demand. Note this is only populated for
	// single zone demands, and it does not guarantee that the demand resources will be scheduled in this zone.
	FulfilledZone string `json:"fulfilled-zone,omitempty"`
}

// ResourceList is a set of (resource name, quantity) pairs.
type ResourceList map[corev1.ResourceName]resource.Quantity

// DemandUnit represents a single unit of demand as a count of resources requirements
type DemandUnit struct {
	Resources ResourceList `json:"resources"`
	Count     int          `json:"count"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DemandList is a list of Demand resources
type DemandList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Demand `json:"items"`
}

// CPU returns the CPU demand if specified.
func (r *ResourceList) CPU() *resource.Quantity {
	if val, ok := (*r)[ResourceCPU]; ok {
		return &val
	}
	return &resource.Quantity{Format: resource.DecimalSI}
}

// Memory returns the Memory demand if specified.
func (r *ResourceList) Memory() *resource.Quantity {
	if val, ok := (*r)[ResourceMemory]; ok {
		return &val
	}
	return &resource.Quantity{Format: resource.DecimalSI}
}

// NvidiaGPU returns the GPU demand if specified.
func (r *ResourceList) NvidiaGPU() *resource.Quantity {
	if val, ok := (*r)[ResourceNvidiaGPU]; ok {
		return &val
	}
	return &resource.Quantity{Format: resource.DecimalSI}
}
