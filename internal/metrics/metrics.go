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
	"errors"
	"time"

	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"k8s.io/api/core/v1"
)

const (
	requestCounter           = "foundry.spark.scheduler.requests"
	schedulingProcessingTime = "foundry.spark.scheduler.schedule.time"
	schedulingWaitTime       = "foundry.spark.scheduler.wait.time"
	schedulingRetryTime      = "foundry.spark.scheduler.retry.time"
	resourceUsageCPU         = "foundry.spark.scheduler.resource.usage.cpu"
	resourceUsageMemory      = "foundry.spark.scheduler.resource.usage.memory"
	lifecycleAgeMax          = "foundry.spark.scheduler.pod.lifecycle.max"
	lifecycleAgeP95          = "foundry.spark.scheduler.pod.lifecycle.p95"
	lifecycleAgeP50          = "foundry.spark.scheduler.pod.lifecycle.p50"
	lifecycleCount           = "foundry.spark.scheduler.pod.lifecycle.count"
	crossAzTraffic           = "foundry.spark.scheduler.cross.az.traffic"
)

const (
	// instanceGroupTagLabel needs to match the labels applied to the kubelets
	instanceGroupTagLabel = "resource_channel"
	sparkRoleLabel        = "spark-role"
	sparkRoleTagName      = "sparkrole"
	outcomeTagName        = "outcome"
	instanceGroupTagName  = "instance-group"
	hostTagName           = "nodename"
	lifecycleTagName      = "lifecycle"
	sparkSchedulerName    = "spark-scheduler"
	nodeZoneLabel         = "failure-domain.beta.kubernetes.io/zone"
)

var (
	didRetryTag = metrics.MustNewTag("retry", "true")
	firstTryTag = metrics.MustNewTag("retry", "false")
)

func ReportCrossZoneMetric(ctx context.Context, driverNodeName string, executorNodeNames []string, nodes []*v1.Node) {
	executorNodesSet := make(map[string]interface{})
	for _, n := range executorNodeNames {
		executorNodesSet[n] = nil
	}

	zonesCounter := make(map[string]int64)
	for _, n := range nodes {
		if _, ok := executorNodesSet[n.Name]; ok {
			executorZone, err := getNodeZone(n)
			if err != nil {
				return
			}
			increment(zonesCounter, executorZone)
		} else if n.Name == driverNodeName {
			driverZone, err := getNodeZone(n)
			if err != nil {
				return
			}
			increment(zonesCounter, driverZone)
		}
	}

	metrics.FromContext(ctx).Histogram(crossAzTraffic).Update(crossZoneTraffic(zonesCounter))
}

func increment(counter map[string]int64, key string) {
	if _, ok := counter[key]; !ok {
		counter[key] = 0
	}
	counter[key] += 1
}

func crossZoneTraffic(zonesCounter map[string]int64) int64 {
	var podsInDifferentZones int64 = 0
	for _, numPods := range zonesCounter {
		podsInDifferentZones += numPods
	}

	var crossZoneTraffic int64 = 0
	for _, numPods := range zonesCounter {
		podsInDifferentZones -= numPods
		crossZoneTraffic += numPods * podsInDifferentZones
	}
	return crossZoneTraffic
}

func getNodeZone(node *v1.Node) (string, error) {
	zone, ok := node.GetLabels()[nodeZoneLabel]
	if !ok {
		return "", errors.New("zone label not found")
	}
	return zone, nil
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

// ScheduleTimer marks pod scheduling time metrics
type ScheduleTimer struct {
	podCreationTime  time.Time
	startTime        time.Time
	lastSeenTime     time.Time
	instanceGroupTag metrics.Tag
	retryTag         metrics.Tag
}

// NewScheduleTimer creates a new ScheduleTimer
func NewScheduleTimer(ctx context.Context, pod *v1.Pod) *ScheduleTimer {
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
		instanceGroupTag: InstanceGroupTag(ctx, pod.Spec.NodeSelector[instanceGroupTagLabel]),
		retryTag:         retryTag,
	}
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
}
