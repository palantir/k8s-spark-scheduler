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
	clientset "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/clientset/versioned"
	ssinformers "github.com/palantir/k8s-spark-scheduler-lib/pkg/client/informers/externalversions"
	"github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/k8s-spark-scheduler/internal/conversionwebhook"
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
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clientcache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "runs the spark scheduler extender server",
	RunE: func(cmd *cobra.Command, args []string) error {
		extender, wc := newWitchcraftServer()
		go extender.StartBackgroundTasksWhenReady()
		err := wc.Start()
		return err
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)
}

type extenderBackgroundTaskHandler struct {
	podInformer                   *coreinformers.PodInformer
	nodeInformer                  *coreinformers.NodeInformer
	resourceReservationInformer   *clientcache.SharedIndexInformer
	resourceReservationCache      *cache.ResourceReservationCache
	lazyDemandInformer            *crd.LazyDemandInformer
	demandCache                   *cache.SafeDemandCache
	wasteMetricsReporter          *metrics.WasteMetricsReporter
	cacheReporter                 *metrics.CacheMetrics
	resourceReporter              *metrics.ResourceUsageReporter
	queueReporter                 *metrics.PendingPodQueueReporter
	softReservationReporter       *metrics.SoftReservationMetrics
	unschedulablePodMarker        *extender.UnschedulablePodMarker
	kubeInformerFactory           *informers.SharedInformerFactory
	sparkSchedulerInformerFactory *ssinformers.SharedInformerFactory
	isReady                       bool
}

func (ext *extenderBackgroundTaskHandler) SetBackgroundClients(
	podInformer *coreinformers.PodInformer,
	nodeInformer *coreinformers.NodeInformer,
	resourceReservationInformer *clientcache.SharedIndexInformer,
	resourceReservationCache *cache.ResourceReservationCache,
	lazyDemandInformer *crd.LazyDemandInformer,
	demandCache *cache.SafeDemandCache,
	wasteMetricsReporter *metrics.WasteMetricsReporter,
	cacheReporter *metrics.CacheMetrics,
	resourceReporter *metrics.ResourceUsageReporter,
	queueReporter *metrics.PendingPodQueueReporter,
	softReservationReporter *metrics.SoftReservationMetrics,
	unschedulablePodMarker *extender.UnschedulablePodMarker,
	kubeInformerFactory *informers.SharedInformerFactory,
	sparkSchedulerInformerFactory *ssinformers.SharedInformerFactory,
) {
	ext.podInformer = podInformer
	ext.nodeInformer = nodeInformer
	ext.resourceReservationInformer = resourceReservationInformer
	ext.resourceReservationCache = resourceReservationCache
	ext.lazyDemandInformer = lazyDemandInformer
	ext.demandCache = demandCache
	ext.wasteMetricsReporter = wasteMetricsReporter
	ext.cacheReporter = cacheReporter
	ext.resourceReporter = resourceReporter
	ext.queueReporter = queueReporter
	ext.softReservationReporter = softReservationReporter
	ext.unschedulablePodMarker = unschedulablePodMarker
	ext.kubeInformerFactory = kubeInformerFactory
	ext.sparkSchedulerInformerFactory = sparkSchedulerInformerFactory
	ext.isReady = true
}

func (ext *extenderBackgroundTaskHandler) StartBackgroundTasksWhenReady() {
	for !ext.isReady {
		time.Sleep(time.Second)
		svc1log.FromContext(context.Background()).Info("Waiting for background initializing to be ready before starting tasks")
	}

	ctx := context.Background()

	go func() {
		_ = wapp.RunWithFatalLogging(ctx, func(ctx context.Context) error {
			(*ext.kubeInformerFactory).Start(ctx.Done())
			return nil
		})
	}()

	go func() {
		_ = wapp.RunWithFatalLogging(ctx, func(ctx context.Context) error {
			(*ext.sparkSchedulerInformerFactory).Start(ctx.Done())
			return nil
		})
	}()

	if ok := clientcache.WaitForCacheSync(
		ctx.Done(),
		(*ext.nodeInformer).Informer().HasSynced,
		(*ext.podInformer).Informer().HasSynced,
		(*ext.resourceReservationInformer).HasSynced); !ok {
		svc1log.FromContext(ctx).Error("Error waiting for cache to sync")
		return
	}

	ext.resourceReservationCache.Run(ctx)
	ext.lazyDemandInformer.Run(ctx)
	ext.demandCache.Run(ctx)
	ext.wasteMetricsReporter.StartSchedulingOverheadMetrics(*ext.podInformer, ext.lazyDemandInformer)
	extender.StartDemandGC(ctx, *ext.podInformer, ext.demandCache)
	go ext.cacheReporter.StartReporting(ctx)
	go ext.resourceReporter.StartReportingResourceUsage(ctx)
	go ext.queueReporter.StartReportingQueues(ctx)
	go ext.softReservationReporter.StartReporting(ctx)
	go ext.unschedulablePodMarker.Start(ctx)
}

func (ext *extenderBackgroundTaskHandler) initServer(ctx context.Context, info witchcraft.InitInfo) (func(), error) {
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
	webhookClientConfig, err := conversionwebhook.InitializeCRDConversionWebhook(ctx, info.Router, install.Server,
		install.WebhookServiceConfig.Namespace, install.WebhookServiceConfig.ServiceName, install.WebhookServiceConfig.ServicePort)
	if err != nil {
		svc1log.FromContext(ctx).Error("Error instantiating CRD conversion webhook: %s", svc1log.Stacktrace(err))
		return nil, err
	}
	err = crd.EnsureResourceReservationsCRD(ctx, apiExtensionsClient, install.ResourceReservationCRDAnnotations,
		v1beta2.ResourceReservationCustomResourceDefinition(webhookClientConfig, v1beta1.ResourceReservationCustomResourceDefinitionVersion()),
	)
	if err != nil {
		svc1log.FromContext(ctx).Error("Error ensuring resource reservations v1beta2 CRD exists: %s", svc1log.Stacktrace(err))
		return nil, err
	}

	kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, time.Second*30)
	sparkSchedulerInformerFactory := ssinformers.NewSharedInformerFactory(sparkSchedulerClient, time.Second*30)

	nodeInformerInterface := kubeInformerFactory.Core().V1().Nodes()
	nodeLister := nodeInformerInterface.Lister()

	podInformerInterface := kubeInformerFactory.Core().V1().Pods()
	podLister := podInformerInterface.Lister()

	resourceReservationInformerInterface := sparkSchedulerInformerFactory.Sparkscheduler().V1beta2().ResourceReservations()
	resourceReservationInformer := resourceReservationInformerInterface.Informer()
	resourceReservationLister := resourceReservationInformerInterface.Lister()

	resourceReservationCache, err := cache.NewResourceReservationCache(
		ctx,
		resourceReservationInformerInterface,
		sparkSchedulerClient.SparkschedulerV1beta2(),
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
		sparkSchedulerClient.ScalerV1alpha2(),
		install.AsyncClientConfig,
	)

	softReservationStore := cache.NewSoftReservationStore(ctx, podInformerInterface)

	sparkPodLister := extender.NewSparkPodLister(podLister, instanceGroupLabel)
	resourceReservationManager := extender.NewResourceReservationManager(ctx, resourceReservationCache, softReservationStore, sparkPodLister, podInformerInterface)

	overheadComputer := extender.NewOverheadComputer(
		ctx,
		podInformerInterface,
		resourceReservationManager,
		nodeLister,
	)

	binpacker := extender.SelectBinpacker(install.BinpackAlgo)

	wasteMetricsReporter := metrics.NewWasteMetricsReporter(ctx, instanceGroupLabel)

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
		install.FifoConfig,
		binpacker,
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

	softReservationReporter := metrics.NewSoftReservationMetrics(ctx, softReservationStore, podLister, resourceReservationCache)

	unschedulablePodMarker := extender.NewUnschedulablePodMarker(
		nodeLister,
		podLister,
		kubeClient.CoreV1(),
		overheadComputer,
		binpacker,
	)

	if err := registerExtenderEndpoints(info.Router, sparkSchedulerExtender); err != nil {
		return nil, err
	}
	ext.SetBackgroundClients(
		&podInformerInterface,
		&nodeInformerInterface,
		&resourceReservationInformer,
		resourceReservationCache,
		lazyDemandInformer,
		demandCache,
		wasteMetricsReporter,
		cacheReporter,
		resourceReporter,
		queueReporter,
		softReservationReporter,
		unschedulablePodMarker,
		&kubeInformerFactory,
		&sparkSchedulerInformerFactory,
	)

	return nil, nil
}

// newWitchcraftServer creates and returns a witchcraft Server.
func newWitchcraftServer() (*extenderBackgroundTaskHandler, *witchcraft.Server) {
	ext := extenderBackgroundTaskHandler{}
	wcServer := witchcraft.NewServer().
		WithInstallConfigType(config.Install{}).
		WithInstallConfigFromFile("var/conf/install.yml").
		// We do this in order to get witchcraft to honor the logging config, which it expects to be in runtime
		WithRuntimeConfigFromFile("var/conf/install.yml").
		WithECVKeyProvider(witchcraft.ECVKeyNoOp()).
		WithInitFunc(ext.initServer).
		WithOrigin(svc1log.CallerPkg(0, 1))
	return &ext, wcServer
}
