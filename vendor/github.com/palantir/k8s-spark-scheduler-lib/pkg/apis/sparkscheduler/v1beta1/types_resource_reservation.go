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

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// InstanceGroupLabel represents the key of a label that defines the name of the instance group
	InstanceGroupLabel = "instance-group"
	// AppIDLabel represents the key of a label that defines the application ID of a pod
	AppIDLabel = "app-id"
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
	Node      string            `json:"node"`
	CPU       resource.Quantity `json:"cpu"`
	Memory    resource.Quantity `json:"memory"`
	NvidiaGPU resource.Quantity `json:"nvidia.com/gpu,omitempty"`
}

// UnmarshalJSON is a custom unmarshal function which makes sure that the NvidiaGPU resource is set correctly.
// It will set the correct format for the resource even if it is omitted from the json.
func (in *Reservation) UnmarshalJSON(data []byte) error {
	// This type is so that we can use the default unmarshalling for all fields apart from NvidiaGPU
	type InnerReservation Reservation
	if err := json.Unmarshal(data, &*(*InnerReservation)(in)); err != nil {
		return err
	}
	in.NvidiaGPU = *resource.NewQuantity(in.NvidiaGPU.Value(), resource.DecimalSI)
	return nil
}

// ResourceReservationStatus shows which reservations are bound to which pod names
type ResourceReservationStatus struct {
	Pods map[string]string `json:"pods"`
}
