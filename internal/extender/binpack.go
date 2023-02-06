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
	singleAZTightlyPack string = "single-az-tightly-pack"

	packFewestBins string = "pack-fewest-bins"
	// Same caveat as for singleAZTightlyPack applies
	singleAZPackFewestBins string = "single-az-pack-fewest-bins"
)

// Binpacker is a BinpackFunc with a known name
type Binpacker struct {
	Name        string
	BinpackFunc binpack.SparkBinPackFunction
}

var binpackFunctions = map[string]*Binpacker{
	tightlyPack:            {tightlyPack, binpack.TightlyPack},
	distributeEvenly:       {distributeEvenly, binpack.DistributeEvenly},
	azAwareTightlyPack:     {azAwareTightlyPack, binpack.AzAwareTightlyPack},
	singleAZTightlyPack:    {singleAZTightlyPack, binpack.SingleAZTightlyPack},
	packFewestBins:         {singleAZTightlyPack, binpack.PackFewestBins},
	singleAZPackFewestBins: {singleAZPackFewestBins, binpack.SingleAzPackFewestBins},
}

// SelectBinpacker selects the binpack function from the given name
func SelectBinpacker(name string) *Binpacker {
	binpacker, ok := binpackFunctions[name]
	if !ok {
		return binpackFunctions[distributeEvenly]
	}
	return binpacker
}
