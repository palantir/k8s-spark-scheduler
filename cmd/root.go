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

package cmd

import (
	"github.com/palantir/pkg/cobracli"
	"github.com/spf13/cobra"
)

var (
	// Version of the program, set via ldflags by godel during build
	Version = "unspecified"

	rootCmd = &cobra.Command{
		Use:   "spark-scheduler",
		Short: "Kube scheduler extender for scheduling spark applications",
	}
)

// Execute runs the application and returns the exit code.
func Execute() int {
	return cobracli.ExecuteWithDefaultParams(rootCmd, cobracli.VersionFlagParam(Version))
}
