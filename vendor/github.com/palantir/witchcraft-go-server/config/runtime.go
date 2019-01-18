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
	"strings"

	"github.com/palantir/witchcraft-go-logging/wlog"
)

// Runtime specifies the base runtime configuration fields that should be included in all witchcraft-server-go
// server runtime configurations.
type Runtime struct {
	HealthChecks HealthChecksConfig `yaml:"health-checks,omitempty"`
	LoggerConfig *LoggerConfig      `yaml:"logging,omitempty"`
}

type HealthChecksConfig struct {
	SharedSecret string `yaml:"shared-secret"`
}

type LoggerConfig struct {
	// Level configures the log level for leveled loggers (such as service logs). Does not impact non-leveled loggers
	// (such as request logs).
	Level wlog.LogLevel `yaml:"level"`
}

func (c *LoggerConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type loggerConfigAlias LoggerConfig
	var cfg loggerConfigAlias
	if err := unmarshal(&cfg); err != nil {
		return err
	}
	*c = LoggerConfig(cfg)
	c.Level = wlog.LogLevel(strings.ToLower(string(c.Level)))
	return nil
}
