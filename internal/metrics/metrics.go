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

package metrics

import (
	"context"
	"net/url"
	"strconv"
	"time"

	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	v1 "k8s.io/api/core/v1"
	clientmetrics "k8s.io/client-go/tools/metrics"
)

const (
	requestCounter                            = "foundry.spark.scheduler.requests"
	schedulingProcessingTime                  = "foundry.spark.scheduler.schedule.time"
	reconciliationTime                        = "foundry.spark.scheduler.reconciliation.time"
	schedulingWaitTime                        = "foundry.spark.scheduler.wait.time"
	schedulingRetryTime                       = "foundry.spark.scheduler.retry.time"
	resourceUsageCPU                          = "foundry.spark.scheduler.resource.usage.cpu"
	resourceUsageMemory                       = "foundry.spark.scheduler.resource.usage.memory"
	resourceUsageNvidiaGPUs                   = "foundry.spark.scheduler.resource.usage.nvidia.com/gpu"
	lifecycleAgeMax                           = "foundry.spark.scheduler.pod.lifecycle.max"
	lifecycleAgeP95                           = "foundry.spark.scheduler.pod.lifecycle.p95"
	lifecycleAgeP50                           = "foundry.spark.scheduler.pod.lifecycle.p50"
	lifecycleCount                            = "foundry.spark.scheduler.pod.lifecycle.count"
	singleAzDynamicAllocationPackFailureCount = "foundry.spark.scheduler.singleazdynamicallocationpackfailure.count"
	crossAzTraffic                            = "foundry.spark.scheduler.az.cross.traffic"
	crossAzTrafficMean                        = "foundry.spark.scheduler.az.cross.traffic.mean"
	totalTraffic                              = "foundry.spark.scheduler.total.traffic"
	totalTrafficMean                          = "foundry.spark.scheduler.total.traffic.mean"
	applicationZonesCount                     = "foundry.spark.scheduler.application.zones.count"
	requestLatency                            = "foundry.spark.scheduler.client.request.latency"
	requestResult                             = "foundry.spark.scheduler.client.request.result"
	cachedObjectCount                         = "foundry.spark.scheduler.cache.objects.count"
	inflightRequestCount                      = "foundry.spark.scheduler.cache.inflight.count"
	unboundCPUReservations                    = "foundry.spark.scheduler.reservations.unbound.cpu"
	unboundMemoryReservations                 = "foundry.spark.scheduler.reservations.unbound.memory"
	unboundNvidiaGPUReservations              = "foundry.spark.scheduler.reservations.unbound.nvidiagpu"
	timeToBind                                = "foundry.spark.scheduler.reservations.timetobind"
	timeToBindP50                             = "foundry.spark.scheduler.reservations.timetobind.p50"
	softReservationCount                      = "foundry.spark.scheduler.softreservation.count"
	softReservationExecutorCount              = "foundry.spark.scheduler.softreservation.executorcount"
	executorsWithNoReservationCount           = "foundry.spark.scheduler.softreservation.executorswithnoreservations"
	softReservationCompactionTime             = "foundry.spark.scheduler.softreservation.compaction.time"
	podInformerDelay                          = "foundry.spark.scheduler.informer.delay"
	schedulingWaste                           = "foundry.spark.scheduler.scheduling.waste"
	schedulingWastePerInstanceGroup           = "foundry.spark.scheduler.scheduling.wasteperinstancegroup"
	initialDriverExecutorCollocation          = "foundry.spark.scheduler.scheduling.initialdriverexecutorcollocation"
	initialExecutorsPerNode                   = "foundry.spark.scheduler.scheduling.initialexecutorspernode"
	initialNodeCount                          = "foundry.spark.scheduler.scheduling.initialnodecount"
)

const (
	sparkRoleLabel             = "spark-role"
	executor                   = "executor"
	sparkRoleTagName           = "sparkrole"
	collocationTypeTagName     = "collocation-type"
	outcomeTagName             = "outcome"
	instanceGroupTagName       = "instance-group"
	hostTagName                = "nodename"
	lifecycleTagName           = "lifecycle"
	sparkSchedulerName         = "spark-scheduler"
	pathTagName                = "requestpath"
	verbTagName                = "requestverb"
	statusCodeTagName          = "requeststatuscode"
	queueIndexTagName          = "queueIndex"
	schedulingWasteTypeTagName = "wastetype"
	zoneTagName                = "zone"
)

const (
	tickInterval     = 30 * time.Second
	slowLogThreshold = 45 * time.Second
)

var (
	didRetryTag = metrics.MustNewTag("retry", "true")
	firstTryTag = metrics.MustNewTag("retry", "false")
)

func init() {
	clientmetrics.Register(clientmetrics.RegisterOpts{})
}

func tagWithDefault(ctx context.Context, key, value, defaultValue string) metrics.Tag {
	tag, err := metrics.NewTag(key, value)
	if err == nil {
		return tag
	}
	svc1log.FromContext(ctx).Error("failed to create metrics tag",
		svc1log.SafeParam("key", key),
		svc1log.SafeParam("value", value),
		svc1log.SafeParam("reason", err.Error()))
	return metrics.MustNewTag(key, defaultValue)
}

// SparkRoleTag returns a spark role tag
func SparkRoleTag(ctx context.Context, role string) metrics.Tag {
	return tagWithDefault(ctx, sparkRoleTagName, role, "unspecified")
}

// CollocationTypeTag returns a collocation type tag
func CollocationTypeTag(ctx context.Context, collocationType string) metrics.Tag {
	return tagWithDefault(ctx, collocationTypeTagName, collocationType, "unspecified")
}

// OutcomeTag returns an outcome tag
func OutcomeTag(ctx context.Context, outcome string) metrics.Tag {
	return tagWithDefault(ctx, outcomeTagName, outcome, "unspecified")
}

// InstanceGroupTag returns an instance group tag
func InstanceGroupTag(ctx context.Context, instanceGroup string) metrics.Tag {
	return tagWithDefault(ctx, instanceGroupTagName, instanceGroup, "unspecified")
}

// HostTag returns a host tag
func HostTag(ctx context.Context, host string) metrics.Tag {
	return tagWithDefault(ctx, hostTagName, host, "unspecified")
}

// PathTag returns a url tag
func PathTag(ctx context.Context, url url.URL) metrics.Tag {
	return tagWithDefault(ctx, pathTagName, url.Path, "unspecified")
}

// VerbTag returns a request verb tag
func VerbTag(ctx context.Context, verb string) metrics.Tag {
	return tagWithDefault(ctx, verbTagName, verb, "unspecified")
}

// StatusCodeTag returns a status code tag
func StatusCodeTag(ctx context.Context, statusCode string) metrics.Tag {
	return tagWithDefault(ctx, statusCodeTagName, statusCode, "unspecified")
}

// ZoneTag returns a zone tag
func ZoneTag(ctx context.Context, zone string) metrics.Tag {
	return tagWithDefault(ctx, zoneTagName, zone, "unspecified")
}

// QueueIndexTag returns a queue index tag
func QueueIndexTag(ctx context.Context, index int) metrics.Tag {
	return tagWithDefault(ctx, queueIndexTagName, strconv.Itoa(index), "unspecified")
}

// ScheduleTimer marks pod scheduling time metrics
type ScheduleTimer struct {
	podCreationTime            time.Time
	startTime                  time.Time
	lastSeenTime               time.Time
	reconciliationFinishedTime time.Time
	instanceGroupTag           metrics.Tag
	retryTag                   metrics.Tag
}

// NewScheduleTimer creates a new ScheduleTimer
func NewScheduleTimer(ctx context.Context, instanceGroup string, pod *v1.Pod) *ScheduleTimer {
	lastSeenTime := pod.CreationTimestamp.Time
	retryTag := firstTryTag
	for _, podCondition := range pod.Status.Conditions {
		if podCondition.Type == v1.PodScheduled {
			lastSeenTime = podCondition.LastTransitionTime.Time
			retryTag = didRetryTag
		}
	}
	return &ScheduleTimer{
		podCreationTime:  pod.CreationTimestamp.Time,
		lastSeenTime:     lastSeenTime,
		startTime:        time.Now(),
		instanceGroupTag: InstanceGroupTag(ctx, instanceGroup),
		retryTag:         retryTag,
	}
}

// MarkReconciliationFinished marks when the reconciliation finished successfully
func (s *ScheduleTimer) MarkReconciliationFinished(ctx context.Context) {
	s.reconciliationFinishedTime = time.Now()
}

// Mark marks scheduling timer metrics with durations from current time
func (s *ScheduleTimer) Mark(ctx context.Context, role, outcome string) {
	sparkRoleTag := SparkRoleTag(ctx, role)
	outcomeTag := OutcomeTag(ctx, outcome)

	metrics.FromContext(ctx).Counter(requestCounter, sparkRoleTag, outcomeTag, s.instanceGroupTag).Inc(1)
	now := time.Now()
	metrics.FromContext(ctx).Histogram(
		schedulingProcessingTime, sparkRoleTag, outcomeTag, s.instanceGroupTag).Update(now.Sub(s.startTime).Nanoseconds())
	metrics.FromContext(ctx).Histogram(
		schedulingWaitTime, sparkRoleTag, outcomeTag, s.instanceGroupTag).Update(now.Sub(s.podCreationTime).Nanoseconds())
	metrics.FromContext(ctx).Histogram(
		schedulingRetryTime, sparkRoleTag, outcomeTag, s.instanceGroupTag, s.retryTag).Update(now.Sub(s.lastSeenTime).Nanoseconds())
	if !s.reconciliationFinishedTime.IsZero() {
		metrics.FromContext(ctx).Histogram(reconciliationTime).Update(s.reconciliationFinishedTime.Sub(s.startTime).Nanoseconds())
	}
	if now.After(s.podCreationTime.Add(slowLogThreshold)) && s.retryTag == firstTryTag {
		svc1log.FromContext(ctx).Info(
			"pod is first seen by the extender, but it is older than the slow log threshold",
			svc1log.SafeParam("slowLogThreshold", slowLogThreshold))
	}
}

// ReportInitialDriverExecutorCollocationMetric reports a metric about whether the driver is collocated with executors.
// This metric is only reported during the initial scheduling of the Spark application.
func ReportInitialDriverExecutorCollocationMetric(ctx context.Context, instanceGroup string, driverNodeName string, executorNodeNames []string) {
	instanceGroupTag := InstanceGroupTag(ctx, instanceGroup)
	executorNodeNamesSet := toExecutorNodeNamesSet(executorNodeNames)

	if !executorNodeNamesSet[driverNodeName] {
		collocationTypeTag := CollocationTypeTag(ctx, "no-collocation")
		metrics.FromContext(ctx).Counter(initialDriverExecutorCollocation, instanceGroupTag, collocationTypeTag).Inc(1)
		return
	}

	if len(executorNodeNamesSet) == 1 {
		collocationTypeTag := CollocationTypeTag(ctx, "driver-collocated-with-all-executors")
		metrics.FromContext(ctx).Counter(initialDriverExecutorCollocation, instanceGroupTag, collocationTypeTag).Inc(1)
		return
	}

	collocationTypeTag := CollocationTypeTag(ctx, "driver-collocated-with-some-executors")
	metrics.FromContext(ctx).Counter(initialDriverExecutorCollocation, instanceGroupTag, collocationTypeTag).Inc(1)
}

// ReportInitialNodeCountMetrics reports two metrics used to reason about how fragmented a Spark app is.
//
// The first metric measures how many executors are hosted per node for a given spark application. This ignores
// executor-less applications.
//
// For instance for an application with 6 executors being scheduled on a single node the metric would be 6,
// if this application was instead scheduled on 2 nodes, then the metric would be 3.
//
// The second metric simply tracks how many unique nodes are being used to schedule all the executors.
//
// Those metrics are only reported during the initial scheduling of the Spark application.
func ReportInitialNodeCountMetrics(ctx context.Context, instanceGroup string, executorNodeNames []string) {
	executorCount := len(executorNodeNames)
	if executorCount == 0 {
		return
	}

	instanceGroupTag := InstanceGroupTag(ctx, instanceGroup)
	executorNodeNamesSet := toExecutorNodeNamesSet(executorNodeNames)
	ratio := executorCount / len(executorNodeNamesSet)

	metrics.FromContext(ctx).Histogram(initialExecutorsPerNode, instanceGroupTag).Update(int64(ratio))
	metrics.FromContext(ctx).Histogram(initialNodeCount, instanceGroupTag).Update(int64(len(executorNodeNamesSet)))
}

func toExecutorNodeNamesSet(executorNodeNames []string) map[string]bool {
	executorNodeNamesSet := make(map[string]bool)
	for _, node := range executorNodeNames {
		executorNodeNamesSet[node] = true
	}
	return executorNodeNamesSet
}

// ReportCrossZoneMetric reports metric about cross AZ traffic between pods of a spark application
func ReportCrossZoneMetric(ctx context.Context, instanceGroup string, driverNodeName string, executorNodeNames []string, nodes []*v1.Node) {
	instanceGroupTag := InstanceGroupTag(ctx, instanceGroup)

	numPodsPerNode := map[string]int{
		driverNodeName: 1,
	}
	for _, n := range executorNodeNames {
		numPodsPerNode[n]++
	}

	numPodsPerZone := make(map[string]int)
	for _, n := range nodes {
		if numPods, ok := numPodsPerNode[n.Name]; ok {
			executorZone, ok := n.Labels[v1.LabelZoneFailureDomain]
			if !ok {
				svc1log.FromContext(ctx).Warn("zone label not found for node", svc1log.SafeParam("nodeName", n.Name))
				executorZone = "unknown-zone"
			}
			numPodsPerZone[executorZone] += numPods
		}
	}

	totalNumPods := len(executorNodeNames) + 1
	crossZonePairs := int64(crossZoneTraffic(numPodsPerZone, totalNumPods))
	totalPairs := int64(totalNumPods * (totalNumPods - 1) / 2)
	numberOfZones := int64(len(numPodsPerZone))

	crossAzPodPairs := metrics.FromContext(ctx).Histogram(crossAzTraffic, instanceGroupTag)
	crossAzPodPairs.Update(crossZonePairs)
	totalPodPairs := metrics.FromContext(ctx).Histogram(totalTraffic, instanceGroupTag)
	totalPodPairs.Update(totalPairs)

	// We care about the mean because we want to see the overall picture of cross AZ scheduling, p95 and p99 are too
	// easily skewed as a small application scheduled across AZs would skew the total cross AZ traffic percentage to be
	// 100% when in reality this represents a fairly small amount of cross AZ traffic
	// We need to explicitly create a metric for this because the mean is stripped from the metric logs of histograms
	// by default, we need to explicitly update it
	metrics.FromContext(ctx).GaugeFloat64(crossAzTrafficMean, instanceGroupTag).Update(crossAzPodPairs.Mean())
	metrics.FromContext(ctx).GaugeFloat64(totalTrafficMean, instanceGroupTag).Update(totalPodPairs.Mean())

	metrics.FromContext(ctx).Histogram(applicationZonesCount, instanceGroupTag).Update(numberOfZones)
}

// crossZoneTraffic calculates the total number of pairs of pods, where the 2 pods are in different zones.
// A pair represents potential cross-zone traffic, which we want to avoid.
func crossZoneTraffic(numPodsPerZone map[string]int, totalNumPods int) int {
	numPodsInDifferentZone := totalNumPods
	var crossZoneTraffic int
	for _, numPodsInZone := range numPodsPerZone {
		numPodsInDifferentZone -= numPodsInZone
		crossZoneTraffic += numPodsInZone * numPodsInDifferentZone
	}
	return crossZoneTraffic
}

type latencyAdapter struct{}

func (l *latencyAdapter) Observe(verb string, u url.URL, latency time.Duration) {
	ctx := context.Background()
	pathTag := PathTag(ctx, u)
	verbTag := VerbTag(ctx, verb)
	metrics.FromContext(ctx).Histogram(requestLatency, pathTag, verbTag).Update(latency.Nanoseconds())
}

type resultAdapter struct{}

func (r *resultAdapter) Increment(code, verb, host string) {
	ctx := context.Background()
	verbTag := VerbTag(ctx, verb)
	statusCodeTag := StatusCodeTag(ctx, code)
	hostTag := HostTag(ctx, host)
	metrics.FromContext(ctx).Counter(requestResult, verbTag, statusCodeTag, hostTag).Inc(1)
}

// SoftReservationCompactionTimer tracks and reports the time it takes to compact soft reservations to resource reservations
type SoftReservationCompactionTimer struct {
	startTime time.Time
}

// GetAndStartSoftReservationCompactionTimer returns a SoftReservationCompactionTimer which starts counting the time immediately
func GetAndStartSoftReservationCompactionTimer() *SoftReservationCompactionTimer {
	return &SoftReservationCompactionTimer{time.Now()}
}

// MarkCompactionComplete emits a metric with the time difference between now and when the timer was started by GetAndStartSoftReservationCompactionTimer()
func (dct *SoftReservationCompactionTimer) MarkCompactionComplete(ctx context.Context) {
	metrics.FromContext(ctx).Histogram(softReservationCompactionTime).Update(time.Now().Sub(dct.startTime).Nanoseconds())
}

// IncrementSingleAzDynamicAllocationPackFailure increments a counter for a zone we fail to schedule in, this allows us to keep track of exactly which zones are over utilised
func IncrementSingleAzDynamicAllocationPackFailure(ctx context.Context, zone string) {
	metrics.FromContext(ctx).Counter(singleAzDynamicAllocationPackFailureCount, ZoneTag(ctx, zone)).Inc(1)
}

// ReportTimeToBindMetrics reports how long it takes between a reservation being created and pods being bound to said reservation.
func ReportTimeToBindMetrics(ctx context.Context, durationMicros int64) {
	timeToBindHist := metrics.FromContext(ctx).Histogram(timeToBind)
	timeToBindHist.Update(durationMicros)
	metrics.FromContext(ctx).GaugeFloat64(timeToBindP50).Update(timeToBindHist.Percentile(.5))
}
