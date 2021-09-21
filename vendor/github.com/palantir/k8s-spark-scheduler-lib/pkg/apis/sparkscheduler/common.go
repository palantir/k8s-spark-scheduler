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

package sparkscheduler

// Constants used across resource reservation versions

const (
	// ResourceReservationPlural defines how to refer to multiple resource reservations
	ResourceReservationPlural = "resourcereservations"

	// GroupName defines the kubernetes group name for resource reservations
	GroupName = "sparkscheduler.palantir.com"

	// ResourceReservationCRDName defines the fully qualified name of the CRD
	ResourceReservationCRDName = ResourceReservationPlural + "." + GroupName

	// ReservationSpecAnnotationKey is the field we set in the object annotation which holds the resource reservation spec
	// in objects with a version < latest version.
	// This is set so that we don't lose information in round trip conversions.
	ReservationSpecAnnotationKey = "scheduler.palantir.github.com/reservation-spec"
)
