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
	"time"

	clientset "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned"
	ssinformers "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/informers/externalversions"
	"github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/k8s-spark-scheduler/internal/crd"
	"github.com/palantir/k8s-spark-scheduler/internal/extender"
	"github.com/palantir/k8s-spark-scheduler/internal/metrics"
	"github.com/palantir/k8s-spark-scheduler/internal/sort"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	"github.com/palantir/witchcraft-go-server/witchcraft"
	"github.com/spf13/cobra"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clientcache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "runs the spark scheduler extender server",
	RunE: func(cmd *cobra.Command, args []string) error {
		return New().Start()
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)
}

func initServer(ctx context.Context, info witchcraft.InitInfo) (func(), error) {
	var kubeconfig *rest.Config
	var err error

	install := info.InstallConfig.(config.Install)
	if install.Kubeconfig != "" {
		kubeconfig, err = clientcmd.BuildConfigFromFlags("", install.Kubeconfig)
		if err != nil {
			svc1log.FromContext(ctx).Error("Error building config from kubeconfig: %s", svc1log.Stacktrace(err))
			return nil, err
		}
	} else {
		kubeconfig, err = rest.InClusterConfig()
		if err != nil {
			svc1log.FromContext(ctx).Error("Error building in cluster kubeconfig: %s", svc1log.Stacktrace(err))
			return nil, err
		}
	}
	kubeconfig.QPS = install.QPS
	kubeconfig.Burst = install.Burst
	instanceGroupLabel := install.InstanceGroupLabel
	if instanceGroupLabel == "" {
		// for back-compat, as instanceGroupLabel was once hard-coded to this value
		instanceGroupLabel = "resource_channel"
	}

	kubeClient, err := kubernetes.NewForConfig(kubeconfig)
	if err != nil {
		svc1log.FromContext(ctx).Error("Error building kubernetes clientset: %s", svc1log.Stacktrace(err))
		return nil, err
	}
	sparkSchedulerClient, err := clientset.NewForConfig(kubeconfig)
	if err != nil {
		svc1log.FromContext(ctx).Error("Error building spark scheduler clientset: %s", svc1log.Stacktrace(err))
		return nil, err
	}
	apiExtensionsClient, err := apiextensionsclientset.NewForConfig(kubeconfig)
	if err != nil {
		svc1log.FromContext(ctx).Error("Error building api extensions clientset: %s", svc1log.Stacktrace(err))
		return nil, err
	}
	err = crd.EnsureResourceReservationsCRD(apiExtensionsClient, install.ResourceReservationCRDAnnotations)
	if err != nil {
		svc1log.FromContext(ctx).Error("Error ensuring resource reservations CRD exists: %s", svc1log.Stacktrace(err))
		return nil, err
	}

	kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, time.Second*30)
	sparkSchedulerInformerFactory := ssinformers.NewSharedInformerFactory(sparkSchedulerClient, time.Second*30)

	nodeInformerInterface := kubeInformerFactory.Core().V1().Nodes()
	nodeInformer := nodeInformerInterface.Informer()
	nodeLister := nodeInformerInterface.Lister()

	podInformerInterface := kubeInformerFactory.Core().V1().Pods()
	podInformer := podInformerInterface.Informer()
	podLister := podInformerInterface.Lister()

	resourceReservationInformerInterface := sparkSchedulerInformerFactory.Sparkscheduler().V1beta1().ResourceReservations()
	resourceReservationInformer := resourceReservationInformerInterface.Informer()
	resourceReservationLister := resourceReservationInformerInterface.Lister()

	go func() {
		_ = wapp.RunWithFatalLogging(ctx, func(ctx context.Context) error {
			kubeInformerFactory.Start(ctx.Done())
			return nil
		})
	}()

	go func() {
		_ = wapp.RunWithFatalLogging(ctx, func(ctx context.Context) error {
			sparkSchedulerInformerFactory.Start(ctx.Done())
			return nil
		})
	}()

	if ok := clientcache.WaitForCacheSync(
		ctx.Done(),
		nodeInformer.HasSynced,
		podInformer.HasSynced,
		resourceReservationInformer.HasSynced); !ok {
		svc1log.FromContext(ctx).Error("Error waiting for cache to sync")
		return nil, nil
	}

	resourceReservationCache, err := cache.NewResourceReservationCache(
		ctx,
		resourceReservationInformerInterface,
		sparkSchedulerClient.SparkschedulerV1beta1(),
		install.AsyncClientConfig,
	)

	if err != nil {
		svc1log.FromContext(ctx).Error("Error constructing resource reservation cache", svc1log.Stacktrace(err))
		return nil, err
	}

	lazyDemandInformer := crd.NewLazyDemandInformer(
		sparkSchedulerInformerFactory,
		apiExtensionsClient,
	)

	demandCache := cache.NewSafeDemandCache(
		lazyDemandInformer,
		sparkSchedulerClient.ScalerV1alpha1(),
		install.AsyncClientConfig,
	)

	extender.StartDemandGC(ctx, podInformerInterface, demandCache)

	softReservationStore := cache.NewSoftReservationStore(ctx, podInformerInterface)

	sparkPodLister := extender.NewSparkPodLister(podLister, instanceGroupLabel)
	resourceReservationManager := extender.NewResourceReservationManager(ctx, resourceReservationCache, softReservationStore, sparkPodLister, podInformerInterface)

	overheadComputer := extender.NewOverheadComputer(
		ctx,
		podLister,
		resourceReservationCache,
		softReservationStore,
		nodeLister,
		instanceGroupLabel,
	)

	binpacker := extender.SelectBinpacker(install.BinpackAlgo)

	sparkSchedulerExtender := extender.NewExtender(
		nodeLister,
		sparkPodLister,
		resourceReservationCache,
		softReservationStore,
		resourceReservationManager,
		kubeClient.CoreV1(),
		demandCache,
		apiExtensionsClient,
		install.FIFO,
		binpacker,
		overheadComputer,
		instanceGroupLabel,
		sort.NewNodeSorter(
			install.DriverPrioritizedNodeLabel,
			install.ExecutorPrioritizedNodeLabel,
		),
	)

	resourceReporter := metrics.NewResourceReporter(
		nodeLister,
		resourceReservationCache,
		instanceGroupLabel,
	)

	metrics.RegisterInformerDelayMetrics(ctx, podInformerInterface)

	cacheReporter := metrics.NewCacheMetrics(
		resourceReservationLister,
		resourceReservationCache,
		demandCache,
	)

	queueReporter := metrics.NewQueueReporter(podLister, instanceGroupLabel)

	softReservationReporter := metrics.NewSoftReservationMetrics(ctx, softReservationStore, podLister, resourceReservationCache)

	unschedulablePodMarker := extender.NewUnschedulablePodMarker(
		nodeLister,
		podLister,
		kubeClient.CoreV1(),
		overheadComputer,
		binpacker,
	)

	resourceReservationCache.Run(ctx)
	lazyDemandInformer.Run(ctx)
	demandCache.Run(ctx)
	metrics.StartSchedulingOverheadMetrics(ctx, podInformerInterface, lazyDemandInformer)
	go cacheReporter.StartReporting(ctx)
	go resourceReporter.StartReportingResourceUsage(ctx)
	go queueReporter.StartReportingQueues(ctx)
	go softReservationReporter.StartReporting(ctx)
	go overheadComputer.Start(ctx)
	go unschedulablePodMarker.Start(ctx)

	if err := registerExtenderEndpoints(info.Router, sparkSchedulerExtender); err != nil {
		return nil, err
	}

	return nil, nil
}

// New creates and returns a witchcraft Server.
func New() *witchcraft.Server {
	return witchcraft.NewServer().
		WithInstallConfigType(config.Install{}).
		WithInstallConfigFromFile("var/conf/install.yml").
		// We do this in order to get witchcraft to honor the logging config, which it expects to be in runtime
		WithRuntimeConfigFromFile("var/conf/install.yml").
		WithSelfSignedCertificate().
		WithECVKeyProvider(witchcraft.ECVKeyNoOp()).
		WithInitFunc(initServer).
		WithOrigin(svc1log.CallerPkg(0, 1))
}
