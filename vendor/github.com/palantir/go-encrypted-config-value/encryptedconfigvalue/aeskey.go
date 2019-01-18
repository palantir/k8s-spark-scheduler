// Copyright 2017 Palantir Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package encryptedconfigvalue

import (
	"github.com/palantir/go-encrypted-config-value/encryption"
)

// NewAESKey creates a new AES key of the specified size and returns a new KeyWithType that is typed as an AES key and
// contains the newly generated key.
func NewAESKey(keySizeBits int) (KeyWithType, error) {
	key, err := encryption.NewAESKey(keySizeBits)
	if err != nil {
		return KeyWithType{}, err
	}
	return AESKeyFromKey(key), nil
}

// AESKeyFromBytes creates a new AES key that uses the provided bytes as its key material and returns a new KeyWithType
// that is typed as an AES key and contains the generated key.
func AESKeyFromBytes(key []byte) KeyWithType {
	return AESKeyFromKey(encryption.AESKeyFromBytes(key))
}

// AESKeyFromKey returns a new KeyWithType that wraps the provided AESKey.
func AESKeyFromKey(key *encryption.AESKey) KeyWithType {
	return KeyWithType{
		Type: AESKey,
		Key:  key,
	}
}

const defaultAESKeySizeBits = 256

// NewAESKeyPair creates a new KeyPair that contains a newly generated AES key of the default size for encrypted-config-value
// (256 bits). The encryption key and decryption key of the key pair are both the same newly generated key.
func NewAESKeyPair() (KeyPair, error) {
	aesKey, err := NewAESKey(defaultAESKeySizeBits)
	if err != nil {
		return KeyPair{}, err
	}
	return KeyPair{
		EncryptionKey: aesKey,
		DecryptionKey: aesKey,
	}, nil
}
