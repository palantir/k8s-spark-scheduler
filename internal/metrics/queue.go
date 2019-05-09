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
	"time"

	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	gometrics "github.com/rcrowley/go-metrics"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
)

const (
	tickInterval   = 30 * time.Second
	decayThreshold = 2 * tickInterval
)

var (
	lifecycleTags = map[v1.PodConditionType]metrics.Tag{
		v1.PodScheduled:   metrics.MustNewTag(lifecycleTagName, "queued"),
		v1.PodInitialized: metrics.MustNewTag(lifecycleTagName, "initializing"),
		v1.PodReady:       metrics.MustNewTag(lifecycleTagName, "ready"),
	}
)

// PendingPodQueueReporter reports queue sizes periodically
type PendingPodQueueReporter struct {
	podLister corelisters.PodLister
}

// NewQueueReporter returns a new ResourceUsageReporter instance
func NewQueueReporter(podLister corelisters.PodLister) *PendingPodQueueReporter {
	return &PendingPodQueueReporter{
		podLister: podLister,
	}
}

// StartReportingQueues starts periodic resource usage reporting
func (r *PendingPodQueueReporter) StartReportingQueues(ctx context.Context) {
	_ = wapp.RunWithFatalLogging(ctx, r.doStart)
}

func (r *PendingPodQueueReporter) doStart(ctx context.Context) error {
	t := time.NewTicker(tickInterval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			pods, err := r.podLister.List(labels.Everything())
			if err != nil {
				svc1log.FromContext(ctx).Error("failed to list pods", svc1log.Stacktrace(err))
				break
			}
			r.report(ctx, pods)
		}
	}
}

func (r *PendingPodQueueReporter) report(ctx context.Context, pods []*v1.Pod) {
	now := time.Now()
	histograms := PodHistograms{}
	for _, pod := range pods {
		if pod.Spec.SchedulerName == sparkSchedulerName {
			histograms.MarkTimes(ctx, pod, now)
		}
	}
	for key, ph := range histograms {
		metrics.FromContext(ctx).Gauge(lifecycleCount, key.InstanceGroup, key.SparkRole, key.Lifecycle).Update(ph.Counter.Count())
		metrics.FromContext(ctx).Gauge(lifecycleAgeMax, key.InstanceGroup, key.SparkRole, key.Lifecycle).Update(ph.Histogram.Max())
		metrics.FromContext(ctx).Gauge(lifecycleAgeP95, key.InstanceGroup, key.SparkRole, key.Lifecycle).Update(int64(ph.Histogram.Percentile(0.95)))
		metrics.FromContext(ctx).Gauge(lifecycleAgeP50, key.InstanceGroup, key.SparkRole, key.Lifecycle).Update(int64(ph.Histogram.Percentile(0.5)))
	}
}

// PodTags represent a tag set for a lifecycle event for a spark pod
type PodTags struct {
	InstanceGroup, SparkRole, Lifecycle metrics.Tag
}

type histogramWithCount struct {
	Histogram gometrics.Histogram
	Counter   gometrics.Counter
}

// PodHistograms keep the count and duration of lifecycle states of a spark pod
type PodHistograms map[PodTags]histogramWithCount

func (p PodHistograms) init(key PodTags) {
	if _, ok := p[key]; !ok {
		p[key] = histogramWithCount{
			gometrics.NewHistogram(metrics.DefaultSample()),
			gometrics.NewCounter(),
		}
	}
}

// Mark marks the histogram for the key with the nanoseconds of given duration
func (p PodHistograms) Mark(key PodTags, duration time.Duration) {
	p.init(key)
	p[key].Histogram.Update(duration.Nanoseconds())
}

// Inc increases the counter of the given key by one
func (p PodHistograms) Inc(key PodTags) {
	p.init(key)
	p[key].Counter.Inc(1)
}

// MarkTimes inspects pod conditions and marks lifecycle transition times
func (p PodHistograms) MarkTimes(ctx context.Context, pod *v1.Pod, now time.Time) {
	instanceGroupTag := InstanceGroupTag(ctx, pod.Spec.NodeSelector[instanceGroupTagLabel])
	sparkRoleTag := SparkRoleTag(ctx, pod.Labels[sparkRoleLabel])
	podConditions := NewSparkPodConditions(pod.Status.Conditions)

	previousStateTime := pod.CreationTimestamp.Time
	for _, state := range []v1.PodConditionType{v1.PodScheduled, v1.PodInitialized, v1.PodReady} {
		stateChangedTime, didChangeState := podConditions.TimeWhenTrue(state)
		key := PodTags{instanceGroupTag, sparkRoleTag, lifecycleTags[state]}
		if !didChangeState {
			p.Mark(key, now.Sub(previousStateTime))
			p.Inc(key)
			return
		}
		if stateChangedTime.Add(decayThreshold).After(now) {
			p.Mark(key, stateChangedTime.Sub(previousStateTime))
		}
		previousStateTime = stateChangedTime
	}
}

// SparkPodConditions provides spark related lifecycle events from pod conditions
type SparkPodConditions map[v1.PodConditionType]v1.PodCondition

// NewSparkPodConditions creates a new SparkPodConditions instance
func NewSparkPodConditions(conditions []v1.PodCondition) SparkPodConditions {
	sparkPodConditions := make(SparkPodConditions, len(conditions))
	for _, cond := range conditions {
		sparkPodConditions[cond.Type] = cond
	}
	return sparkPodConditions
}

// TimeWhenTrue returns the last transition time if the given conditions status is true
func (s SparkPodConditions) TimeWhenTrue(conditionType v1.PodConditionType) (time.Time, bool) {
	if cond, ok := s[conditionType]; ok && cond.Status == v1.ConditionTrue {
		return cond.LastTransitionTime.Time, true
	}
	return time.Time{}, false
}
