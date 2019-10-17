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

package events

const (
	// ApplicationScheduled is an event emitted when an application has been successfully scheduled. This usually means
	// we have created the necessary resource reservations for the driver and all executors.
	ApplicationScheduled = "foundry.spark.scheduler.application_scheduled"
	// DemandCreated is an event emitted when we create a Demand object for an application. This means that we have
	// asked the cluster for more resources than are currently provisioned.
	DemandCreated = "foundry.spark.scheduler.demand_created"
	// DemandDeleted is an event emitted when we delete a Demand object for an application. This means that we have
	// relinquished our request for more resources than are currently provisioned, either because we don't need them
	// anymore  or because we have received the resources we requested.
	DemandDeleted = "foundry.spark.scheduler.demand_deleted"
)
