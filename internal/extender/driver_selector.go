package extender

import (
	"context"
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/palantir/k8s-spark-scheduler/config"
	"github.com/palantir/k8s-spark-scheduler/internal/cache"
	"github.com/palantir/k8s-spark-scheduler/internal/common"
	"github.com/palantir/k8s-spark-scheduler/internal/common/utils"
	"github.com/palantir/k8s-spark-scheduler/internal/metrics"
	ns "github.com/palantir/k8s-spark-scheduler/internal/sort"
	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	v1affinityhelper "k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"time"
)

type NodeSelector interface {
	SelectNode(
		ctx context.Context,
		instanceGroup string,
		driver *v1.Pod,
		nodeNames []string) (string, error)
}

type driverSelector struct {
	overheadComputer           *OverheadComputer
	resourceReservationManager *ResourceReservationManager
	nodeLister                 corelisters.NodeLister
	isFIFO                     bool
	fifoConfig                 config.FifoConfig
	demands                    *cache.SafeDemandCache
	podLister                  *SparkPodLister
	nodeSorter                 *ns.NodeSorter
	binpacker                  *Binpacker
}

func (s *driverSelector) SelectNode(
	ctx context.Context,
	instanceGroup string,
	driver *v1.Pod,
	nodeNames []string) (string, error) {
	if rr, ok := s.resourceReservationManager.GetResourceReservation(driver.Labels[common.SparkAppIDLabel], driver.Namespace); ok {
		driverReservedNode := rr.Spec.Reservations["driver"].Node
		for _, node := range nodeNames {
			if driverReservedNode == node {
				svc1log.FromContext(ctx).Info("Received request to schedule driver which already has a reservation. Returning previously reserved node.",
					svc1log.SafeParam("driverReservedNode", driverReservedNode))
				return driverReservedNode, nil
			}
		}
		svc1log.FromContext(ctx).Warn("Received request to schedule driver which already has a reservation, but previously reserved node is not in list of nodes. Returning previously reserved node anyway.",
			svc1log.SafeParam("driverReservedNode", driverReservedNode),
			svc1log.SafeParam("nodeNames", nodeNames))
		return driverReservedNode, nil
	}
	availableNodes, err := utils.ListWithPredicate(s.nodeLister, func(node *v1.Node) (bool, error) {
		match, error := v1affinityhelper.GetRequiredNodeAffinity(driver).Match(node)
		return match, error
	})
	if err != nil {
		return "", err
	}

	usage := s.resourceReservationManager.GetReservedResources()
	overhead := s.overheadComputer.GetOverhead(ctx, availableNodes)

	availableNodesSchedulingMetadata := resources.NodeSchedulingMetadataForNodes(availableNodes, usage, overhead)
	driverNodeNames, executorNodeNames := s.nodeSorter.PotentialNodes(availableNodesSchedulingMetadata, nodeNames)
	applicationResources, err := sparkResources(ctx, driver)
	if err != nil {
		return "", werror.Wrap(err, "failed to get spark resources")
	}
	if s.isFIFO {
		queuedDrivers, err := s.podLister.ListEarlierDrivers(driver)
		if err != nil {
			return "", werror.Wrap(err, "failed to list earlier drivers")
		}
		ok := s.fitEarlierDrivers(ctx, instanceGroup, queuedDrivers, driverNodeNames, executorNodeNames, availableNodesSchedulingMetadata)
		if !ok {
			// TODO
			// s.createDemandForApplicationInAnyZone(ctx, driver, applicationResources)
			return "", werror.Error("earlier drivers do not fit to the cluster")
		}
	}

	packingResult := s.binpacker.BinpackFunc(
		ctx,
		applicationResources.driverResources,
		applicationResources.executorResources,
		applicationResources.minExecutorCount,
		driverNodeNames,
		executorNodeNames,
		availableNodesSchedulingMetadata)
	efficiency := computeAvgPackingEfficiencyForResult(availableNodesSchedulingMetadata, packingResult)

	svc1log.FromContext(ctx).Debug("binpacking result",
		svc1log.SafeParam("availableNodesSchedulingMetadata", availableNodesSchedulingMetadata),
		svc1log.SafeParam("driverResources", applicationResources.driverResources),
		svc1log.SafeParam("executorResources", applicationResources.executorResources),
		svc1log.SafeParam("minExecutorCount", applicationResources.minExecutorCount),
		svc1log.SafeParam("maxExecutorCount", applicationResources.maxExecutorCount),
		svc1log.SafeParam("hasCapacity", packingResult.HasCapacity),
		svc1log.SafeParam("candidateDriverNodes", nodeNames),
		svc1log.SafeParam("candidateExecutorNodes", executorNodeNames),
		svc1log.SafeParam("driverNode", packingResult.DriverNode),
		svc1log.SafeParam("executorNodes", packingResult.ExecutorNodes),
		svc1log.SafeParam("avg packing efficiency CPU", efficiency.CPU),
		svc1log.SafeParam("avg packing efficiency Memory", efficiency.Memory),
		svc1log.SafeParam("avg packing efficiency GPU", efficiency.GPU),
		svc1log.SafeParam("avg packing efficiency Max", efficiency.Max),
		svc1log.SafeParam("binpacker", s.binpacker.Name))
	if !packingResult.HasCapacity {
		// TODO s.createDemandForApplicationInAnyZone(ctx, driver, applicationResources)
		return "", werror.ErrorWithContextParams(ctx, "application does not fit to the cluster")
	}

	metrics.ReportPackingEfficiency(ctx, instanceGroup, s.binpacker.Name, efficiency)

	s.removeDemandIfExists(ctx, driver)
	metrics.ReportInitialDriverExecutorCollocationMetric(ctx, instanceGroup, packingResult.DriverNode, packingResult.ExecutorNodes)
	metrics.ReportInitialNodeCountMetrics(ctx, instanceGroup, packingResult.ExecutorNodes)
	metrics.ReportCrossZoneMetric(ctx, instanceGroup, packingResult.DriverNode, packingResult.ExecutorNodes, availableNodes)

	_, err = s.resourceReservationManager.CreateReservations(
		ctx,
		driver,
		applicationResources,
		packingResult.DriverNode,
		packingResult.ExecutorNodes,
	)
	if err != nil {
		return "", err
	}
	return packingResult.DriverNode, nil
}

// fitEarlierDrivers binpacks all given spark applications to the cluster and
// accounts for their resource usage in availableNodesSchedulingMetadata
func (s *driverSelector) fitEarlierDrivers(
	ctx context.Context,
	instanceGroup string,
	drivers []*v1.Pod,
	nodeNames, executorNodeNames []string,
	availableNodesSchedulingMetadata resources.NodeGroupSchedulingMetadata) bool {
	for _, driver := range drivers {
		applicationResources, err := sparkResources(ctx, driver)
		if err != nil {
			svc1log.FromContext(ctx).Warn("failed to get driver resources, skipping driver",
				svc1log.SafeParam("faultyDriverName", driver.Name),
				svc1log.SafeParam("reason", err.Error))
			continue
		}
		packingResult := s.binpacker.BinpackFunc(
			ctx,
			applicationResources.driverResources,
			applicationResources.executorResources,
			applicationResources.minExecutorCount,
			nodeNames, executorNodeNames, availableNodesSchedulingMetadata)
		if !packingResult.HasCapacity {
			if s.shouldSkipDriverFifo(driver, instanceGroup) {
				svc1log.FromContext(ctx).Debug("Skipping non-fitting driver from FIFO consideration because it is not too old yet",
					svc1log.SafeParam("earlierDriverName", driver.Name))
				continue
			}
			svc1log.FromContext(ctx).Warn("failed to fit one of the earlier drivers",
				svc1log.SafeParam("earlierDriverName", driver.Name))
			return false
		}

		availableNodesSchedulingMetadata.SubtractUsageIfExists(sparkResourceUsage(
			applicationResources.driverResources,
			applicationResources.executorResources,
			packingResult.DriverNode,
			packingResult.ExecutorNodes))
	}
	return true
}

func (s *driverSelector) removeDemandIfExists(ctx context.Context, pod *v1.Pod) {
	DeleteDemandIfExists(ctx, s.demands, pod, "SparkSchedulerExtender")
}

func (s *driverSelector) shouldSkipDriverFifo(pod *v1.Pod, instanceGroup string) bool {
	enforceAfterPodAgeConfig := s.fifoConfig.DefaultEnforceAfterPodAge
	if instanceGroupCustomConfig, ok := s.fifoConfig.EnforceAfterPodAgeByInstanceGroup[instanceGroup]; ok {
		enforceAfterPodAgeConfig = instanceGroupCustomConfig
	}
	return pod.CreationTimestamp.Add(enforceAfterPodAgeConfig).After(time.Now())
}
