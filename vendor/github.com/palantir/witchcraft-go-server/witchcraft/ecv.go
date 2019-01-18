// Copyright (c) 2018 Palantir Technologies. All rights reserved.
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

package witchcraft

import (
	"io/ioutil"
	"os"

	"github.com/palantir/go-encrypted-config-value/encryptedconfigvalue"
	"github.com/palantir/witchcraft-go-error"
)

type ECVKeyProvider interface {
	Load() (*encryptedconfigvalue.KeyWithType, error)
}

type ecvKeyProvider func() (*encryptedconfigvalue.KeyWithType, error)

func (p ecvKeyProvider) Load() (*encryptedconfigvalue.KeyWithType, error) {
	return p()
}

func ECVKeyNoOp() ECVKeyProvider {
	return ECVKeyFromStatic(nil)
}

func ECVKeyFromStatic(kwt *encryptedconfigvalue.KeyWithType) ECVKeyProvider {
	return ecvKeyProvider(func() (*encryptedconfigvalue.KeyWithType, error) {
		return kwt, nil
	})
}

func ECVKeyFromFile(path string) ECVKeyProvider {
	return ecvKeyProvider(func() (*encryptedconfigvalue.KeyWithType, error) {
		keyBytes, err := ioutil.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, werror.Wrap(err, "encryption key file does not exist", werror.SafeParam("path", path))
			}
			return nil, werror.Wrap(err, "failed to read encryption key file", werror.SafeParam("path", path))
		}
		kwt, err := encryptedconfigvalue.NewKeyWithType(string(keyBytes))
		if err != nil {
			return nil, werror.Wrap(err, "failed to create key with type")
		}
		return &kwt, nil
	})
}
