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

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta2"
	ssinformers "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/informers/externalversions"
	"github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/k8s-spark-scheduler/internal/binpacker"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/k8s-spark-scheduler/internal/conversionwebhook"
	"github.com/palantir/k8s-spark-scheduler/internal/crd"
	"github.com/palantir/k8s-spark-scheduler/internal/demands"
	"github.com/palantir/k8s-spark-scheduler/internal/extender"
	"github.com/palantir/k8s-spark-scheduler/internal/metrics"
	"github.com/palantir/k8s-spark-scheduler/internal/sort"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	"github.com/palantir/witchcraft-go-server/witchcraft"
	"github.com/spf13/cobra"
	"k8s.io/client-go/informers"
	clientcache "k8s.io/client-go/tools/cache"
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
	install := info.InstallConfig.(config.Install)
	allClient, err := GetClients(ctx, install)
	if err != nil {
		return nil, err
	}
	err = InitServerWithClients(ctx, info, allClient)
	return nil, err
}

// InitServerWithClients is exported for end to end testing
func InitServerWithClients(ctx context.Context, info witchcraft.InitInfo, allClient AllClient) error {
	install := info.InstallConfig.(config.Install)
	instanceGroupLabel := install.InstanceGroupLabel
	if instanceGroupLabel == "" {
		// for back-compat, as instanceGroupLabel was once hard-coded to this value
		instanceGroupLabel = "resource_channel"
	}

	apiExtensionsClient := allClient.APIExtensionsClient
	sparkSchedulerClient := allClient.SparkSchedulerClient
	kubeClient := allClient.KubeClient

	webhookClientConfig, err := conversionwebhook.InitializeCRDConversionWebhook(ctx, info.Router, install.Server,
		install.WebhookServiceConfig.Namespace, install.WebhookServiceConfig.ServiceName, install.WebhookServiceConfig.ServicePort)
	if err != nil {
		svc1log.FromContext(ctx).Error("Error instantiating CRD conversion webhook: %s", svc1log.Stacktrace(err))
		return err
	}
	err = crd.EnsureResourceReservationsCRD(ctx, apiExtensionsClient, install.ResourceReservationCRDAnnotations,
		v1beta2.ResourceReservationCustomResourceDefinition(webhookClientConfig, v1beta1.ResourceReservationCustomResourceDefinitionVersion()),
	)
	if err != nil {
		svc1log.FromContext(ctx).Error("Error ensuring resource reservations v1beta2 CRD exists: %s", svc1log.Stacktrace(err))
		return err
	}

	kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, time.Second*30)
	sparkSchedulerInformerFactory := ssinformers.NewSharedInformerFactory(sparkSchedulerClient, time.Second*30)

	nodeInformerInterface := kubeInformerFactory.Core().V1().Nodes()
	nodeInformer := nodeInformerInterface.Informer()
	nodeLister := nodeInformerInterface.Lister()

	podInformerInterface := kubeInformerFactory.Core().V1().Pods()
	podInformer := podInformerInterface.Informer()
	podLister := podInformerInterface.Lister()

	resourceReservationInformerInterface := sparkSchedulerInformerFactory.Sparkscheduler().V1beta2().ResourceReservations()
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
		return werror.ErrorWithContextParams(ctx, "could not sync")
	}

	resourceReservationCache, err := cache.NewResourceReservationCache(
		ctx,
		resourceReservationInformerInterface,
		sparkSchedulerClient.SparkschedulerV1beta2(),
		install.AsyncClientConfig,
	)

	if err != nil {
		svc1log.FromContext(ctx).Error("Error constructing resource reservation cache", svc1log.Stacktrace(err))
		return err
	}

	lazyDemandInformer := crd.NewLazyDemandInformer(
		sparkSchedulerInformerFactory,
		apiExtensionsClient,
	)
	binpacker := binpacker.SelectBinpacker(install.BinpackAlgo)
	demandCache := cache.NewSafeDemandCache(
		lazyDemandInformer,
		sparkSchedulerClient.ScalerV1alpha2(),
		install.AsyncClientConfig,
	)
	demandManager := demands.NewDefaultManager(
		demandCache,
		binpacker,
		instanceGroupLabel)
	extender.StartDemandGC(ctx, podInformerInterface, demandManager)

	softReservationStore := cache.NewSoftReservationStore(ctx, podInformerInterface)

	sparkPodLister := extender.NewSparkPodLister(podLister, instanceGroupLabel)
	resourceReservationManager := extender.NewResourceReservationManager(ctx, resourceReservationCache, softReservationStore, sparkPodLister, podInformerInterface)

	overheadComputer := extender.NewOverheadComputer(
		ctx,
		podInformerInterface,
		resourceReservationManager,
		nodeLister,
	)

	wasteMetricsReporter := metrics.NewWasteMetricsReporter(ctx, instanceGroupLabel)

	sparkSchedulerExtender := extender.NewExtender(
		nodeLister,
		sparkPodLister,
		resourceReservationCache,
		softReservationStore,
		resourceReservationManager,
		kubeClient.CoreV1(),
		demandManager,
		apiExtensionsClient,
		install.FIFO,
		install.FifoConfig,
		binpacker,
		install.ShouldScheduleDynamicallyAllocatedExecutorsInSameAZ,
		overheadComputer,
		instanceGroupLabel,
		sort.NewNodeSorter(
			install.DriverPrioritizedNodeLabel,
			install.ExecutorPrioritizedNodeLabel,
		),
		wasteMetricsReporter,
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

	resourceReservationReporter := metrics.NewResourceReservationMetrics(resourceReservationCache)
	softReservationReporter := metrics.NewSoftReservationMetrics(ctx, softReservationStore, podLister, resourceReservationCache)

	unschedulablePodMarker := extender.NewUnschedulablePodMarker(
		nodeLister,
		podLister,
		kubeClient.CoreV1(),
		overheadComputer,
		binpacker,
		install.UnschedulablePodTimeoutDuration,
	)

	resourceReservationCache.Run(ctx)
	lazyDemandInformer.Run(ctx)
	demandCache.Run(ctx)
	wasteMetricsReporter.StartSchedulingOverheadMetrics(podInformerInterface, lazyDemandInformer)
	go cacheReporter.StartReporting(ctx)
	go resourceReporter.StartReportingResourceUsage(ctx)
	go queueReporter.StartReportingQueues(ctx)
	go resourceReservationReporter.StartReporting(ctx)
	go softReservationReporter.StartReporting(ctx)
	go unschedulablePodMarker.Start(ctx)

	if err := registerExtenderEndpoints(info.Router, sparkSchedulerExtender); err != nil {
		return err
	}

	return nil
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
