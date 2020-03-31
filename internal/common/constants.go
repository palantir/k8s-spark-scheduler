// Copyright (c) 2020 Palantir Technologies. All rights reserved.
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

package common

const (
	// SparkSchedulerName is the name of the kube-scheduler instance that talks with the extender
	SparkSchedulerName = "spark-scheduler"
	// SparkRoleLabel represents the label key for the spark-role of a pod
	SparkRoleLabel = "spark-role"
	// SparkAppIDLabel represents the label key for the spark application ID on a pod
	SparkAppIDLabel = "spark-app-id" // TODO(onursatici): change this to a spark specific label when spark has one
	// Driver represents the label key for a pod that identifies the pod as a spark driver
	Driver = "driver"
	// Executor represents the label key for a pod that identifies the pod as a spark executor
	Executor = "executor"
)

const (
	// DriverCPU represents the key of an annotation that describes how much CPU a spark driver requires
	DriverCPU = "spark-driver-cpu"
	// DriverMemory represents the key of an annotation that describes how much memory a spark driver requires
	DriverMemory = "spark-driver-mem"
	// ExecutorCPU represents the key of an annotation that describes how much cpu a spark executor requires
	ExecutorCPU = "spark-executor-cpu"
	// ExecutorMemory represents the key of an annotation that describes how much memory a spark executor requires
	ExecutorMemory = "spark-executor-mem"
	// DynamicAllocationEnabled sets whether dynamic allocation is enabled for this spark application (false by default)
	DynamicAllocationEnabled = "spark-dynamic-allocation-enabled"
	// ExecutorCount represents the key of an annotation that describes how many executors a spark application requires (required if DynamicAllocationEnabled is false)
	ExecutorCount = "spark-executor-count"
	// DAMinExecutorCount represents the lower bound on the number of executors a spark application requires if dynamic allocation is enabled (required if DynamicAllocationEnabled is true)
	DAMinExecutorCount = "spark-dynamic-allocation-min-executor-count"
	// DAMaxExecutorCount represents the upper bound on the number of executors a spark application can have if dynamic allocation is enabled (required if DynamicAllocationEnabled is true)
	DAMaxExecutorCount = "spark-dynamic-allocation-max-executor-count"
)
