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

package extender

import (
	"github.com/palantir/k8s-spark-scheduler-lib/pkg/binpack"
)

const (
	distributeEvenly   string = "distribute-evenly"
	tightlyPack        string = "tightly-pack"
	azAwareTightlyPack string = "az-aware-tightly-pack"

	// Note that single-az-tightly-pack does not guarantee that ALL pods will be scheduled in the same AZ, please see
	// the SingleAzTightlyPack docs for more information
	singleAZTightlyPack          string = "single-az-tightly-pack"
	singleAzMinimalFragmentation string = "single-az-minimal-fragmentation"
)

// Binpacker is a BinpackFunc with a known name
type Binpacker struct {
	Name        string
	BinpackFunc binpack.SparkBinPackFunction
	IsSingleAz  bool
	// RequiredSingledAz denotes that an application should only ever be scheduled in a single AZ (if IsSingleAz is true)
	// otherwise this is merely a preference.
	RequiredSingleAz bool
}

var binpackFunctions = map[string]*Binpacker{
	tightlyPack:                  {Name: tightlyPack, BinpackFunc: binpack.TightlyPack},
	distributeEvenly:             {Name: distributeEvenly, BinpackFunc: binpack.DistributeEvenly},
	azAwareTightlyPack:           {Name: azAwareTightlyPack, BinpackFunc: binpack.AzAwareTightlyPack, IsSingleAz: true},
	singleAZTightlyPack:          {Name: singleAZTightlyPack, BinpackFunc: binpack.SingleAZTightlyPack, IsSingleAz: true, RequiredSingleAz: true},
	singleAzMinimalFragmentation: {Name: singleAzMinimalFragmentation, BinpackFunc: binpack.SingleAZMinimalFragmentation, IsSingleAz: true, RequiredSingleAz: true},
}

// SelectBinpacker selects the binpack function from the given name
func SelectBinpacker(name string) *Binpacker {
	binpacker, ok := binpackFunctions[name]
	if !ok {
		return binpackFunctions[distributeEvenly]
	}
	return binpacker
}
