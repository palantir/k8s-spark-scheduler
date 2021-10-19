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

package config

import (
	"time"

	"github.com/palantir/witchcraft-go-server/config"
)

// Install contains the install time configuration of the server and kubernetes dependency
type Install struct {
	config.Install     `yaml:",inline"`
	config.Runtime     `yaml:",inline"`
	Kubeconfig         string            `yaml:"kube-config,omitempty"`
	FIFO               bool              `yaml:"fifo,omitempty"`
	FifoConfig         FifoConfig        `yaml:"fifo-config,omitempty"`
	QPS                float32           `yaml:"qps,omitempty"`
	Burst              int               `yaml:"burst,omitempty"`
	BinpackAlgo        string            `yaml:"binpack,omitempty"`
	InstanceGroupLabel string            `yaml:"instance-group-label,omitempty"`
	AsyncClientConfig  AsyncClientConfig `yaml:"async-client-config,omitempty"`

	// Deprecated: assumed true, value not used
	UseExperimentalHostPriorities bool `yaml:"use-experimental-host-priorities,omitempty"`

	DriverPrioritizedNodeLabel   *LabelPriorityOrder `yaml:"driver-prioritized-node-label,omitempty"`
	ExecutorPrioritizedNodeLabel *LabelPriorityOrder `yaml:"executor-prioritized-node-label,omitempty"`

	ResourceReservationCRDAnnotations map[string]string `yaml:"resource-reservation-crd-annotations,omitempty"`

	WebhookServiceConfig `yaml:"webhook_service_config"`
}

// WebhookServiceConfig specifies the k8s service which the api server will call to convert between different versions
// of the ResourceReservation CRD
type WebhookServiceConfig struct {
	Namespace   string `yaml:"namespace"`
	ServiceName string `yaml:"service-name"`
	ServicePort int32  `yaml:"service-port"`
}

// FifoConfig enables the fine-tuning of FIFO enforcement
type FifoConfig struct {
	// DefaultEnforceAfterPodAge specifies the time since the pod was created after which a driver which does not fit starts blocking the remaining drivers
	// (Default is 0, i.e. we always block the queue)
	DefaultEnforceAfterPodAge time.Duration `yaml:"default-enforce-after-pod-age,omitempty"`
	// EnforceAfterPodAgeByInstanceGroup allows customizing the fifo enforcement after pod age by instance group
	EnforceAfterPodAgeByInstanceGroup map[string]time.Duration `yaml:"enforce-after-pod-age-by-instance-group,omitempty"`
}

// AsyncClientConfig is the configuration for the internal async client
type AsyncClientConfig struct {
	maxRetryCount *int `yaml:"max-retry-count,omitempty"`
}

// MaxRetryCount returns the maximum number of times the internal async client retries calls to the api server
func (acc AsyncClientConfig) MaxRetryCount() int {
	if acc.maxRetryCount == nil || *acc.maxRetryCount < 0 {
		return 5
	}
	return *acc.maxRetryCount
}

// LabelPriorityOrder is the configuration for denoting an ordered list of values of a node label that will
// be used to sort candidate nodes for drivers or executors
type LabelPriorityOrder struct {
	Name                     string   `yaml:"label-name"`
	DescendingPriorityValues []string `yaml:"label-values-descending-priority"`
}
