// Copyright 2017 Palantir Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package encryption

import (
	"fmt"
)

// AESKey is an AES key that can be used for AES encryption and decryption operations.
type AESKey struct {
	key []byte
}

func NewAESKey(keySizeBits int) (*AESKey, error) {
	k, err := RandomBytes(keySizeBits / 8)
	if err != nil {
		return nil, fmt.Errorf("failed to generate random bytes for AES key: %v", err)
	}
	return AESKeyFromBytes(k), nil
}

func AESKeyFromBytes(key []byte) *AESKey {
	return &AESKey{
		key: key,
	}
}

func (a *AESKey) Bytes() []byte {
	return a.key
}
