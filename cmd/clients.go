package cmd

import (
	"context"
	clientset "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned"
	"github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type AllClient struct {
	ApiExtensionsClient  apiextensionsclientset.Interface
	SparkSchedulerClient clientset.Interface
	KubeClient           kubernetes.Interface
}

func GetClients(ctx context.Context, install config.Install) (AllClient, error) {
	var kubeconfig *rest.Config
	var err error
	if install.Kubeconfig != "" {
		kubeconfig, err = clientcmd.BuildConfigFromFlags("", install.Kubeconfig)
		if err != nil {
			svc1log.FromContext(ctx).Error("Error building config from kubeconfig: %s", svc1log.Stacktrace(err))
			return AllClient{}, err
		}
	} else {
		kubeconfig, err = rest.InClusterConfig()
		if err != nil {
			svc1log.FromContext(ctx).Error("Error building in cluster kubeconfig: %s", svc1log.Stacktrace(err))
			return AllClient{}, err
		}
	}
	kubeconfig.QPS = install.QPS
	kubeconfig.Burst = install.Burst

	kubeClient, err := kubernetes.NewForConfig(kubeconfig)
	if err != nil {
		svc1log.FromContext(ctx).Error("Error building kubernetes clientset: %s", svc1log.Stacktrace(err))
		return AllClient{}, err
	}
	sparkSchedulerClient, err := clientset.NewForConfig(kubeconfig)
	if err != nil {
		svc1log.FromContext(ctx).Error("Error building spark scheduler clientset: %s", svc1log.Stacktrace(err))
		return AllClient{}, err
	}
	apiExtensionsClient, err := apiextensionsclientset.NewForConfig(kubeconfig)
	if err != nil {
		svc1log.FromContext(ctx).Error("Error building api extensions clientset: %s", svc1log.Stacktrace(err))
		return AllClient{}, err
	}
	return AllClient{
		ApiExtensionsClient:  apiExtensionsClient,
		SparkSchedulerClient: sparkSchedulerClient,
		KubeClient:           kubeClient,
	}, nil
}
