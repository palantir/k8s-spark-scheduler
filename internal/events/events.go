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

import (
	"context"
	"time"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha2"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/witchcraft-go-logging/wlog/evtlog/evt2log"
	v1 "k8s.io/api/core/v1"
)

const (
	applicationScheduled = "foundry.spark.scheduler.application_scheduled"
	demandCreated        = "foundry.spark.scheduler.demand_created"
	demandDeleted        = "foundry.spark.scheduler.demand_deleted"
)

// EmitApplicationScheduled logs an event when an application has been successfully scheduled. This usually means
// we have created the necessary resource reservations for the driver and all executors.
func EmitApplicationScheduled(
	ctx context.Context,
	instanceGroup string,
	sparkAppID string,
	pod v1.Pod,
	driverResources *resources.Resources,
	executorResources *resources.Resources,
	minExecutorCount int,
	maxExecutorCount int,
) {
	evt2log.FromContext(ctx).Event(applicationScheduled, evt2log.Values(map[string]interface{}{
		"instanceGroup":      instanceGroup,
		"sparkAppID":         sparkAppID,
		"driverCpu":          driverResources.CPU.Value(),
		"driverMemory":       driverResources.Memory.Value(),
		"driverNvidiaGpus":   driverResources.NvidiaGPU.Value(),
		"executorCpu":        executorResources.CPU.Value(),
		"executorMemory":     executorResources.Memory.Value(),
		"executorNvidiaGpus": executorResources.NvidiaGPU.Value(),
		"minExecutorCount":   minExecutorCount,
		"maxExecutorCount":   maxExecutorCount,
	}))
}

// EmitDemandCreated logs an event when we create a Demand object for an application. This means that we have
// asked the cluster for more resources than are currently provisioned.
func EmitDemandCreated(ctx context.Context, demand *v1alpha2.Demand) {
	evt2log.FromContext(ctx).Event(demandCreated, evt2log.Values(map[string]interface{}{
		"instanceGroup":   demand.Spec.InstanceGroup,
		"demandNamespace": demand.Namespace,
		"demandName":      demand.Name,
	}))
}

// EmitDemandDeleted logs an event when we delete a Demand object for an application. This means that we have
// relinquished our request for more resources than are currently provisioned, either because we don't need them
// anymore  or because we have received the resources we requested.
func EmitDemandDeleted(ctx context.Context, demand *v1alpha2.Demand, source string) {
	demandAge := time.Now().UTC().Sub(demand.CreationTimestamp.UTC())
	evt2log.FromContext(ctx).Event(demandDeleted, evt2log.Values(map[string]interface{}{
		"instanceGroup":      demand.Spec.InstanceGroup,
		"demandNamespace":    demand.Namespace,
		"demandName":         demand.Name,
		"demandAgeSeconds":   int(demandAge.Seconds()),
		"demandCreationTime": demand.CreationTimestamp.UTC(),
		"source":             source,
	}))
}
