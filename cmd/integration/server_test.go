package integration

import (
	"context"
	ssclientset "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned/fake"
	"github.com/palantir/k8s-spark-scheduler/cmd"
	config2 "github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	"github.com/palantir/witchcraft-go-server/config"
	"github.com/palantir/witchcraft-go-server/witchcraft"
	"github.com/stretchr/testify/require"
	extensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"os"
	"testing"
	"time"
)

func Test_InitServerWithClients(t *testing.T) {
	// ctx := context.Background()
	allClients := cmd.AllClient{
		ApiExtensionsClient:  extensionsfake.NewSimpleClientset(),
		SparkSchedulerClient: ssclientset.NewSimpleClientset(),
		KubeClient:           k8sfake.NewSimpleClientset(),
	}

	installConfig := config2.Install{}

	server := witchcraft.NewServer().
		WithInstallConfigType(config2.Install{}).
		WithInstallConfig(installConfig).
		WithRuntimeConfig(config.Runtime{}).
		WithLoggerStdoutWriter(os.Stdout).
		WithDisableGoRuntimeMetrics().
		WithInitFunc(func(ctx context.Context, initInfo witchcraft.InitInfo) (func(), error) {
			f := func(ctx context.Context) error {
				_, err := cmd.InitServerWithClients(ctx, initInfo, allClients)
				require.NoError(t, err)
				return err
			}
			err := wapp.RunWithFatalLogging(ctx, f)
			require.NoError(t, err)
			return nil, err
		})

	go func() {
		server.Start()
	}()
	time.Sleep(time.Second * 3)
}
