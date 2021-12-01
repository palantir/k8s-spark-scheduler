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
	"context"
	"github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/k8s-spark-scheduler/resource-reservation-conversion-webhook/internal/conversionwebhook"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-server/witchcraft"
	"github.com/spf13/cobra"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "runs the resource reservation conversion webhook server",
	RunE: func(cmd *cobra.Command, args []string) error {
		return New().Start()
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)
}

func initServer(ctx context.Context, info witchcraft.InitInfo) (func(), error) {
	err := conversionwebhook.InitializeCRDConversionWebhook(ctx, info.Router)
	return nil, err
}

// New creates and returns a witchcraft Server.
func New() *witchcraft.Server {
	return witchcraft.NewServer().
		WithInstallConfigType(config.Install{}).
		WithInstallConfigFromFile("var/conf/install.yml").
		// We do this in order to get witchcraft to honor the logging config, which it expects to be in runtime
		WithRuntimeConfigFromFile("var/conf/install.yml").
		WithECVKeyProvider(witchcraft.ECVKeyNoOp()).
		WithInitFunc(initServer).
		WithOrigin(svc1log.CallerPkg(0, 1))
}
