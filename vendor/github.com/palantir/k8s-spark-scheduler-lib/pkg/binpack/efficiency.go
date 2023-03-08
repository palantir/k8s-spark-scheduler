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

package binpack

import (
	"math"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
)

// AvgPackingEfficiency represents result packing efficiency per resource type for a group of nodes.
// Computed as average packing efficiency over all node efficiencies.
type AvgPackingEfficiency struct {
	CPU    float64
	Memory float64
	GPU    float64
	Max    float64
}

// LessThan compares two average packing efficiencies. For a single packing we take the highest of the
// resources' efficiency. For example, when CPU is at 0.81 and Memory is at 0.54 the avg efficiency
// is 0.81. One packing efficiency is deemed less efficient when its avg efficiency is lower than
// the other's packing efficiency.
func (p *AvgPackingEfficiency) LessThan(o AvgPackingEfficiency) bool {
	return p.Max < o.Max
}

// WorstAvgPackingEfficiency returns a representation of a failed bin packing. Each individual resource
// type is at worst possible (zero) packing efficiency.
func WorstAvgPackingEfficiency() AvgPackingEfficiency {
	return AvgPackingEfficiency{
		CPU:    0.0,
		Memory: 0.0,
		GPU:    0.0,
		Max:    0.0,
	}
}

// PackingEfficiency represents result packing efficiency per resource type for one node. Computed
// as the total resources used divided by total capacity.
type PackingEfficiency struct {
	NodeName string
	CPU      float64
	Memory   float64
	GPU      float64
}

// Max returns the highest packing efficiency of all resources.
func (p *PackingEfficiency) Max() float64 {
	return math.Max(p.GPU, math.Max(p.CPU, p.Memory))
}

// ComputePackingEfficiencies calculates utilization for all provided nodes, given the new reservation.
func ComputePackingEfficiencies(
	nodeGroupSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	reservedResources resources.NodeGroupResources) map[string]*PackingEfficiency {

	nodeEfficiencies := make(map[string]*PackingEfficiency, 0)

	for nodeName, nodeSchedulingMetadata := range nodeGroupSchedulingMetadata {
		nodeEfficiencies[nodeName] = computePackingEfficiency(nodeName, *nodeSchedulingMetadata, reservedResources)
	}

	return nodeEfficiencies
}

func computePackingEfficiency(
	nodeName string,
	nodeSchedulingMetadata resources.NodeSchedulingMetadata,
	reservedResources resources.NodeGroupResources) *PackingEfficiency {

	nodeReservedResources := nodeSchedulingMetadata.SchedulableResources.Copy()
	nodeReservedResources.Sub(nodeSchedulingMetadata.AvailableResources)
	if reserved, ok := reservedResources[nodeName]; ok {
		nodeReservedResources.Add(reserved)
	}
	nodeSchedulableResources := nodeSchedulingMetadata.SchedulableResources

	// GPU treated differently because not every node has GPU
	gpuEfficiency := 0.0
	if nodeSchedulableResources.NvidiaGPU.Value() != 0 {
		gpuEfficiency = float64(nodeReservedResources.NvidiaGPU.Value()) / float64(normalizeResource(nodeSchedulableResources.NvidiaGPU.Value()))
	}

	return &PackingEfficiency{
		NodeName: nodeName,
		CPU:      float64(nodeReservedResources.CPU.Value()) / float64(normalizeResource(nodeSchedulableResources.CPU.Value())),
		Memory:   float64(nodeReservedResources.Memory.Value()) / float64(normalizeResource(nodeSchedulableResources.Memory.Value())),
		GPU:      gpuEfficiency,
	}
}

func normalizeResource(resourceValue int64) int64 {
	if resourceValue == 0 {
		return 1
	}
	return resourceValue
}

// ComputeAvgPackingEfficiency calculate average packing efficiency, given packing efficiencies for
// individual nodes.
func ComputeAvgPackingEfficiency(
	nodeGroupSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	packingEfficiencies []*PackingEfficiency) AvgPackingEfficiency {

	if len(packingEfficiencies) == 0 {
		return WorstAvgPackingEfficiency()
	}

	var cpuSum, gpuSum, memorySum, maxSum float64
	nodesWithGPU := 0

	for _, packingEfficiency := range packingEfficiencies {
		nodeName := packingEfficiency.NodeName
		nodeSchedulingMetadata := nodeGroupSchedulingMetadata[nodeName]

		cpuSum += packingEfficiency.CPU
		memorySum += packingEfficiency.Memory

		if nodeSchedulingMetadata.SchedulableResources.NvidiaGPU.Value() != 0 {
			gpuSum += packingEfficiency.GPU
			nodesWithGPU++
		}

		maxSum += math.Max(packingEfficiency.GPU, math.Max(packingEfficiency.CPU, packingEfficiency.Memory))
	}

	length := math.Max(float64(len(packingEfficiencies)), 1)
	var gpuEfficiency float64
	if nodesWithGPU == 0 {
		gpuEfficiency = 1
	} else {
		gpuEfficiency = gpuSum / float64(nodesWithGPU)
	}

	avgEfficiency := AvgPackingEfficiency{
		CPU:    cpuSum / length,
		Memory: memorySum / length,
		GPU:    gpuEfficiency,
		Max:    maxSum / length,
	}

	return avgEfficiency
}
