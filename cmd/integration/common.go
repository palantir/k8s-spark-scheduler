package integration

import (
	"context"
	demandapi "github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/scaler/v1alpha2"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta2"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
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
)

type testSetup struct {
	ref     *extender.SparkSchedulerExtender
	ctx     context.Context
	cleanup func()
}

func setUpServer(ctx context.Context, t *testing.T, installConfig config2.Install, allClients cmd.AllClient) testSetup {
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

	waitForCondition(ctx, t, func() bool {
		return ref != nil
	})

	cleanup := func() {
		if err := server.Close(); err != nil {
			svc1log.FromContext(ctx).Error(err.Error(), svc1log.Stacktrace(err))
		}
	}

	return testSetup{
		ref:     ref,
		ctx:     rootCtx,
		cleanup: cleanup,
	}
}

func getReadyCRDs() []runtime.Object {
	crd := v1beta2.ResourceReservationCustomResourceDefinition(&v1.WebhookClientConfig{}, v1beta1.ResourceReservationCustomResourceDefinitionVersion())
	crd.Status = v1.CustomResourceDefinitionStatus{
		Conditions: []v1.CustomResourceDefinitionCondition{
			{
				Type:   v1.Established,
				Status: v1.ConditionTrue,
			},
		},
	}
	demandCRD := demandapi.DemandCustomResourceDefinition(nil)
	demandCRD.Status = v1.CustomResourceDefinitionStatus{
		Conditions: []v1.CustomResourceDefinitionCondition{
			{
				Type:   v1.Established,
				Status: v1.ConditionTrue,
			},
		},
	}
	return []runtime.Object{crd, demandCRD}
}

func getBool(b bool) *bool {
	return &b
}

func toResource(parse resource.Quantity) *resource.Quantity {
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
