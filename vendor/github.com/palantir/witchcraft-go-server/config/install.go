// Copyright (c) 2018 Palantir Technologies. All rights reserved.
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
)

// Install specifies the base install configuration fields that should be included in all witchcraft-go-server server
// install configurations.
type Install struct {
	ProductName          string        `yaml:"product-name,omitempty"`
	Server               Server        `yaml:"server,omitempty"`
	MetricsEmitFrequency time.Duration `yaml:"metrics-emit-frequency,omitempty"`
	UseConsoleLog        bool          `yaml:"use-console-log,omitempty"`
}

type Server struct {
	Address        string   `yaml:"address,omitempty"`
	Port           int      `yaml:"port,omitempty" `
	ManagementPort int      `yaml:"management-port,omitempty" `
	ContextPath    string   `yaml:"context-path,omitempty"`
	ClientCAFiles  []string `yaml:"client-ca-files,omitempty"`
	CertFile       string   `yaml:"cert-file,omitempty"`
	KeyFile        string   `yaml:"key-file,omitempty"`
}
