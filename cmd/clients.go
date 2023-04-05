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

	clientset "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned"
	"github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// AllClient holds all the k8s clients used
type AllClient struct {
	APIExtensionsClient  apiextensionsclientset.Interface
	SparkSchedulerClient clientset.Interface
	KubeClient           kubernetes.Interface
}

// GetClients creates AllClient given the passed in install config
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
		APIExtensionsClient:  apiExtensionsClient,
		SparkSchedulerClient: sparkSchedulerClient,
		KubeClient:           kubeClient,
	}, nil
}
