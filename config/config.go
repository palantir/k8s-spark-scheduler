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
	"github.com/palantir/witchcraft-go-server/config"
)

// Install contains the install time configuration of the server and kubernetes dependency
type Install struct {
	config.Install                `yaml:",inline"`
	config.Runtime                `yaml:",inline"`
	Kubeconfig                    string            `yaml:"kube-config,omitempty"`
	FIFO                          bool              `yaml:"fifo,omitempty"`
	QPS                           float32           `yaml:"qps,omitempty"`
	Burst                         int               `yaml:"burst,omitempty"`
	BinpackAlgo                   string            `yaml:"binpack,omitempty"`
	InstanceGroupLabel            string            `yaml:"instance-group-label,omitempty"`
	AsyncClientConfig             AsyncClientConfig `yaml:"async-client-config,omitempty"`
	UseExperimentalHostPriorities bool              `yaml:"use-experimental-host-priorities,omitempty"`

	ResourceReservationCRDAnnotations map[string]string `yaml:"resource-reservation-crd-annotations,omitempty"`
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
