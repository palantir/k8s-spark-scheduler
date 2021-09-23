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

package v1beta2

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ResourceCPU is the name of CPU resource.
	ResourceCPU corev1.ResourceName = corev1.ResourceCPU
	// ResourceMemory is the name of Memory resource.
	ResourceMemory corev1.ResourceName = corev1.ResourceMemory
	// ResourceNvidiaGPU is the name of Nvidia GPU resource.
	ResourceNvidiaGPU corev1.ResourceName = "nvidia.com/gpu"
)

var (
	// SupportedV1Beta2ResourceTypes is the list of resources explicitly used for scheduling in this version of scheduler
	SupportedV1Beta2ResourceTypes = [...]corev1.ResourceName{ResourceCPU, ResourceMemory, ResourceNvidiaGPU}
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceReservationList represents a list of ResourceReservations
type ResourceReservationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []ResourceReservation `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceReservation is a collection of reservation objects for a distributed application
type ResourceReservation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ResourceReservationSpec   `json:"spec"`
	Status ResourceReservationStatus `json:"status"`
}

// ResourceReservationSpec represents reservations for the driver and executors
// of an application
type ResourceReservationSpec struct {
	Reservations map[string]Reservation `json:"reservations"`
}

// Reservation represents the reserved node and resources for a single process of
// a distributed application
type Reservation struct {
	Node      string       `json:"node"`
	Resources ResourceList `json:"resources"`
}

// ResourceReservationStatus shows which reservations are bound to which pod names
type ResourceReservationStatus struct {
	Pods map[string]string `json:"pods"`
}

// ResourceList maps from a resource type to a quantity, e.g. CPU:1
type ResourceList map[string]*resource.Quantity

// CPU returns the number of cores for the reservation, if cores have not been specified, it returns 0.
func (r ResourceList) CPU() *resource.Quantity {
	if val, ok := r[string(ResourceCPU)]; ok {
		return val
	}
	return resource.NewQuantity(0, resource.DecimalSI)
}

// Memory returns the amount of memory for the reservation, if memory has not been specified, it returns 0.
func (r *ResourceList) Memory() *resource.Quantity {
	if val, ok := (*r)[string(ResourceMemory)]; ok {
		return val
	}
	return resource.NewQuantity(0, resource.BinarySI)
}

// NvidiaGPU returns the amount of Nvidia GPUs for the reservation, if Nvidia GPUs have not been specified, it returns 0.
func (r *ResourceList) NvidiaGPU() *resource.Quantity {
	if val, ok := (*r)[string(ResourceNvidiaGPU)]; ok {
		return val
	}
	return resource.NewQuantity(0, resource.DecimalSI)
}
