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

package binpacker

import (
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/binpack"
)

const (
	distributeEvenly   string = "distribute-evenly"
	tightlyPack        string = "tightly-pack"
	azAwareTightlyPack string = "az-aware-tightly-pack"

	// SingleAzTightlyPack will attempt to schedule pods within a single AZ
	// Note that single-az-tightly-pack does not guarantee that ALL pods will be scheduled in the same AZ, please see
	// the SingleAzTightlyPack docs for more information (see also ShouldScheduleDynamicallyAllocatedExecutorsInSameAZ)
	SingleAzTightlyPack string = "single-az-tightly-pack"
	// SingleAzMinimalFragmentation tries to reduce spark app fragmentation by trying to fit executors on fewer hosts
	// when possible. Dynamically allocated executors are a bit more challenging, but generally speaking we will
	// attempt to schedule them on host already running executors belonging to the same app.
	SingleAzMinimalFragmentation string = "single-az-minimal-fragmentation"
)

// Binpacker is a BinpackFunc with a known name
type Binpacker struct {
	Name        string
	BinpackFunc binpack.SparkBinPackFunction
	IsSingleAz  bool
}

var binpackFunctions = map[string]*Binpacker{
	tightlyPack:                  {tightlyPack, binpack.TightlyPack, false},
	distributeEvenly:             {distributeEvenly, binpack.DistributeEvenly, false},
	azAwareTightlyPack:           {azAwareTightlyPack, binpack.AzAwareTightlyPack, false},
	SingleAzTightlyPack:          {SingleAzTightlyPack, binpack.SingleAZTightlyPack, true},
	SingleAzMinimalFragmentation: {SingleAzMinimalFragmentation, binpack.SingleAZMinimalFragmentation, true},
}

// SelectBinpacker selects the binpack function from the given name
func SelectBinpacker(name string) *Binpacker {
	binpacker, ok := binpackFunctions[name]
	if !ok {
		return binpackFunctions[distributeEvenly]
	}
	return binpacker
}
