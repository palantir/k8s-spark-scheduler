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

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/palantir/k8s-spark-scheduler/cmd"
	config2 "github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/k8s-spark-scheduler/internal/extender"
	"github.com/palantir/witchcraft-go-logging/wlog"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	"github.com/palantir/witchcraft-go-server/config"
	"github.com/palantir/witchcraft-go-server/witchcraft"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestSetup returns values needed for setting up the server
type TestSetup struct {
	ref     *extender.SparkSchedulerExtender
	ctx     context.Context
	cleanup func()
}

// SetUpServer sets up a WitchcraftServer invokes the init function with the client set
func SetUpServer(ctx context.Context, t *testing.T, installConfig config2.Install, allClients cmd.AllClient) TestSetup {
	var ref *extender.SparkSchedulerExtender
	var rootCtx context.Context
	server := witchcraft.NewServer().
		WithInstallConfigType(config2.Install{}).
		WithInstallConfig(installConfig).
		WithSelfSignedCertificate().
		WithRuntimeConfig(config.Runtime{
			LoggerConfig: &config.LoggerConfig{
				Level: wlog.DebugLevel,
			},
		}).
		WithDisableGoRuntimeMetrics().
		WithInitFunc(func(ctx context.Context, initInfo witchcraft.InitInfo) (func(), error) {
			rootCtx = ctx
			f := func(ctx context.Context) error {
				var err error
				ref, err = cmd.InitServerWithClients(ctx, initInfo, allClients)
				require.NoError(t, err)
				return err
			}
			err := wapp.RunWithFatalLogging(ctx, f)
			require.NoError(t, err)
			return nil, err
		})
	go func() {
		err := server.Start()
		require.NoError(t, err)
	}()
	// wait to ensure that there is an extension to return
	waitForCondition(ctx, t, func() bool {
		// Ensure that CRDs are all marked as ready
		crds, err := allClients.APIExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().List(context.Background(), metav1.ListOptions{})
		require.NoError(t, err)
		for _, crd := range crds.Items {
			crd.Status.Conditions = []apiextensionsv1.CustomResourceDefinitionCondition{
				{
					Type:   apiextensionsv1.Established,
					Status: apiextensionsv1.ConditionTrue,
				},
			}
			_, err = allClients.APIExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Update(context.Background(), &crd, metav1.UpdateOptions{})
			require.NoError(t, err)
		}
		return ref != nil
	})

	cleanup := func() {
		if err := server.Close(); err != nil {
			svc1log.FromContext(ctx).Error(err.Error(), svc1log.Stacktrace(err))
		}
	}

	return TestSetup{
		ref:     ref,
		ctx:     rootCtx,
		cleanup: cleanup,
	}
}

// ToResource de-references a Quantity
func ToResource(parse resource.Quantity) *resource.Quantity {
	return &parse
}

func waitForCondition(ctx context.Context, t *testing.T, condition func() bool) {
	ticker := time.NewTicker(time.Millisecond * 10)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second*5))
	defer ticker.Stop()
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			require.Fail(t, "Did not resolve condition")
			return
		case <-ticker.C:
			checkCorrect := condition()
			if checkCorrect {
				return
			}
		}
	}
}

// GetAffinityForInstanceGroup creates an Affinity for an instance group
func GetAffinityForInstanceGroup(labelKey string, instanceGroupName string) *v1.Affinity {
	return &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{
								Key:    labelKey,
								Values: []string{instanceGroupName},
							},
						},
					},
				},
			},
		},
	}
}
