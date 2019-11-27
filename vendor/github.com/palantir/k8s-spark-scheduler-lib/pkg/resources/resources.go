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

package resources

import (
	"time"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/apis/sparkscheduler/v1beta1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	zoneLabelPlaceholder = "default"
)

// UsageForNodes tallies resource usages per node from the given list of resource reservations
func UsageForNodes(resourceReservations []*v1beta1.ResourceReservation) NodeGroupResources {
	res := NodeGroupResources(map[string]*Resources{})
	for _, rr := range resourceReservations {
		for _, reservation := range rr.Spec.Reservations {
			node := reservation.Node
			if res[node] == nil {
				res[node] = Zero()
			}
			res[node].AddFromReservation(&reservation)
		}
	}
	return res
}

// AvailableForNodes finds available resources by subtracting current usage from allocatable per node
func AvailableForNodes(nodes []*v1.Node, currentUsage NodeGroupResources) NodeGroupResources {
	res := NodeGroupResources(make(map[string]*Resources, len(nodes)))
	for _, n := range nodes {
		currentUsageForNode, ok := currentUsage[n.Name]
		if !ok {
			currentUsageForNode = Zero()
		}
		res[n.Name] = subtractFromResourceList(n.Status.Allocatable, currentUsageForNode)
	}
	return res
}

// NodeSchedulingMetadataForNodes calculate available resources by subtracting current usage from allocatable per node
func NodeSchedulingMetadataForNodes(nodes []*v1.Node, currentUsage NodeGroupResources) NodeGroupSchedulingMetadata {
	nodeGroupSchedulingMetadata := make(NodeGroupSchedulingMetadata, len(nodes))
	for _, node := range nodes {
		currentUsageForNode, ok := currentUsage[node.Name]
		if !ok {
			currentUsageForNode = Zero()
		}
		zoneLabel, ok := node.Labels[v1.LabelZoneFailureDomain]
		if !ok {
			zoneLabel = zoneLabelPlaceholder
		}
		nodeGroupSchedulingMetadata[node.Name] = &NodeSchedulingMetadata{
			AvailableResources: subtractFromResourceList(node.Status.Allocatable, currentUsageForNode),
			CreationTimestamp:  node.CreationTimestamp.Time,
			ZoneLabel:          zoneLabel,
			Unschedulable:      node.Spec.Unschedulable,
		}
	}
	return nodeGroupSchedulingMetadata
}

// NodeGroupResources represents resources for a group of nodes
type NodeGroupResources map[string]*Resources

// NodeGroupSchedulingMetadata represents NodeSchedulingMetadata for a group of nodes
type NodeGroupSchedulingMetadata map[string]*NodeSchedulingMetadata

// Add adds all resources in other into the receiver, modifies receiver
func (nodeResources NodeGroupResources) Add(other NodeGroupResources) {
	for node, r := range other {
		if _, ok := nodeResources[node]; !ok {
			nodeResources[node] = Zero()
		}
		nodeResources[node].Add(r)
	}
}

// Sub subtract all resources in other from the receiver, modifies receiver
func (nodeResources NodeGroupResources) Sub(other NodeGroupResources) {
	for node, r := range other {
		if _, ok := nodeResources[node]; !ok {
			nodeResources[node] = Zero()
		}
		nodeResources[node].Sub(r)
	}
}

// SubtractUsageIfExists subtracts usedResourcesByNodeName from the receiver, modifies receiver, only for nodes that exist in receiver
func (nodesSchedulingMetadata NodeGroupSchedulingMetadata) SubtractUsageIfExists(usedResourcesByNodeName NodeGroupResources) {
	for nodeName, usedResources := range usedResourcesByNodeName {
		if nodeSchedulingMetadata, ok := nodesSchedulingMetadata[nodeName]; ok {
			nodeSchedulingMetadata.AvailableResources.Sub(usedResources)
		}
	}
}

func subtractFromResourceList(resourceList v1.ResourceList, resources *Resources) *Resources {
	// (a - b) == -(b - a)
	copyResources := resources.Copy()
	copyResources.CPU.Sub(resourceList[v1.ResourceCPU])
	copyResources.CPU.Neg()
	copyResources.Memory.Sub(resourceList[v1.ResourceMemory])
	copyResources.Memory.Neg()
	return copyResources
}

// Resources represents the CPU and Memory resource quantities
type Resources struct {
	CPU    resource.Quantity
	Memory resource.Quantity
}

// NodeSchedulingMetadata represents various parameters of a node that are considered in scheduling decisions
type NodeSchedulingMetadata struct {
	AvailableResources *Resources
	CreationTimestamp  time.Time
	ZoneLabel          string
	Unschedulable      bool
}

// Zero returns a Resources object with quantities of zero
func Zero() *Resources {
	return &Resources{
		CPU:    *resource.NewQuantity(0, resource.DecimalSI),
		Memory: *resource.NewQuantity(0, resource.BinarySI),
	}
}

//AddFromReservation modifies the receiver in place.
func (r *Resources) AddFromReservation(reservation *v1beta1.Reservation) {
	r.CPU.Add(reservation.CPU)
	r.Memory.Add(reservation.Memory)
}

// Copy returns a clone of the Resources object
func (r *Resources) Copy() *Resources {
	return &Resources{
		CPU:    *r.CPU.Copy(),
		Memory: *r.Memory.Copy(),
	}
}

//Add modifies the receiver in place.
func (r *Resources) Add(other *Resources) {
	r.CPU.Add(other.CPU)
	r.Memory.Add(other.Memory)
}

//Sub modifies the receiver in place
func (r *Resources) Sub(other *Resources) {
	r.CPU.Sub(other.CPU)
	r.Memory.Sub(other.Memory)
}

// AddFromResourceList modified the receiver in place
func (r *Resources) AddFromResourceList(resourceList v1.ResourceList) {
	r.CPU.Add(resourceList[v1.ResourceCPU])
	r.Memory.Add(resourceList[v1.ResourceMemory])
}

// GreaterThan returns true if either the CPU or Memory quantities of this object are greater than those
// of other
func (r *Resources) GreaterThan(other *Resources) bool {
	return r.CPU.Cmp(other.CPU) > 0 || r.Memory.Cmp(other.Memory) > 0
}

// Eq returns true if both CPU and Memory quantities are equal between this Resources object and other
func (r *Resources) Eq(other *Resources) bool {
	return r.CPU.Cmp(other.CPU) == 0 && r.Memory.Cmp(other.Memory) == 0
}
