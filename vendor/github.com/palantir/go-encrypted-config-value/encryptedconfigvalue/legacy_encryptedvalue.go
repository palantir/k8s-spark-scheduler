// Copyright 2017 Palantir Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package encryptedconfigvalue

import (
	"fmt"

	"github.com/palantir/go-encrypted-config-value/encryption"
)

type legacyEncryptedValue struct {
	encryptedBytes []byte
}

// Decrypt decrypts this value using the provided key. Because legacy values do not track the type of the encrypted value
// they contain, it will attempt to decrypt its content based on the key that is provided to the Decrypt function.
func (ev *legacyEncryptedValue) Decrypt(key KeyWithType) (string, error) {
	ciphertext := ev.encryptedBytes
	switch key.Key.(type) {
	default:
		return "", fmt.Errorf("key type %T not supported", key.Key)
	case *encryption.AESKey:
		aesGCMEV := &aesGCMEncryptedValue{
			encrypted: ciphertext[aesGCMLegacyNonceSizeBytes : len(ciphertext)-aesGCMLegacyTagSizeBytes],
			nonce:     ciphertext[:aesGCMLegacyNonceSizeBytes],
			tag:       ciphertext[len(ciphertext)-aesGCMLegacyTagSizeBytes:],
		}
		return aesGCMEV.Decrypt(key)
	case *encryption.RSAPrivateKey:
		rsaOAEPEV := &rsaOAEPEncryptedValue{
			encrypted:   ciphertext,
			oaepHashAlg: rsaOAEPLegacyOAEPHash,
			mdf1HashAlg: rsaOAEPLegacyMDF1Hash,
		}
		return rsaOAEPEV.Decrypt(key)
	}
}

// ToSerializable returns the serializable representation for this legacy encrypted value, which is of the form:
// "enc:<base64-encoded-ciphertext-bytes>". For AES values, the ciphertext bytes are "nonce+ciphertext+tag", while for
// RSA values the ciphertext is the raw ciphertext.
func (ev *legacyEncryptedValue) ToSerializable() SerializedEncryptedValue {
	return newSerializedEncryptedValue(ev.encryptedBytes)
}
