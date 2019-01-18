// Copyright 2017 Palantir Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package encryption

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"hash"
)

// HashAlgorithm represents a hash algorithm.
type HashAlgorithm string

const (
	SHA1   = HashAlgorithm("SHA-1")
	SHA256 = HashAlgorithm("SHA-256")
)

func (a HashAlgorithm) Hash() hash.Hash {
	switch a {
	case "SHA-1":
		return sha1.New()
	case "SHA-256":
		return sha256.New()
	default:
		panic(fmt.Errorf("unrecognized HashAlgorithm: %s", a))
	}
}

// RSAOAEPCipher is a cipher that supports encrypting values using RSA public keys and decrypting values using RSA
// private keys. Uses OAEP/MDF1 padding with the specified hash algorithms.
type RSAOAEPCipher struct {
	oaepHashAlg HashAlgorithm
	mdf1HashAlg HashAlgorithm
}

// OAEPHashAlg returns the hash algorithm used for the OAEP padding for this cipher.
func (r *RSAOAEPCipher) OAEPHashAlg() HashAlgorithm {
	return r.oaepHashAlg
}

// MDF1HashAlg returns the hash algorithm used for the MDF1 XOR operation in the OAEP padding for this cipher.
func (r *RSAOAEPCipher) MDF1HashAlg() HashAlgorithm {
	return r.mdf1HashAlg
}

// NewRSAOAEPCipher returns a new Cipher that uses RSA with OAEP/MDF1 padding using default parameters (SHA-256 as the
// OAEP hash algorithm and MDF1 as the MDF1 hash algorithm).
func NewRSAOAEPCipher() Cipher {
	return &RSAOAEPCipher{
		oaepHashAlg: SHA256,
		mdf1HashAlg: SHA256,
	}
}

// RSAOAEPCipherWithAlgorithms returns a new Cipher that uses RSA with OAEP/MDF1 padding using the specified hash
// algorithms for OAEP and MDF1 padding.
func RSAOAEPCipherWithAlgorithms(oaepHashAlg, mdf1HashAlg HashAlgorithm) *RSAOAEPCipher {
	return &RSAOAEPCipher{
		oaepHashAlg: oaepHashAlg,
		mdf1HashAlg: mdf1HashAlg,
	}
}

// Encrypt encrypts the provided value using the specified key. The key must be of type *RSAPublicKey. The returned
// bytes are the encrypted ciphertext.
func (r *RSAOAEPCipher) Encrypt(data []byte, key Key) ([]byte, error) {
	pubKey, ok := key.(*RSAPublicKey)
	if !ok {
		return nil, fmt.Errorf("key must be of *RSAPublicKey, but was %T", key)
	}
	encrypted, err := encryptOAEP(r.oaepHashAlg.Hash(), r.mdf1HashAlg.Hash(), rand.Reader, (*rsa.PublicKey)(pubKey), data, []byte{})
	if err != nil {
		return nil, err
	}
	return encrypted, nil
}

// Decrypt decrypts the provided value using the specified key. The key must be of type *RSAPrivateKey.
func (r *RSAOAEPCipher) Decrypt(data []byte, key Key) ([]byte, error) {
	privKey, ok := key.(*RSAPrivateKey)
	if !ok {
		return nil, fmt.Errorf("key must be of type *RSAPrivateKey, was %T", key)
	}
	decrypted, err := decryptOAEP(r.oaepHashAlg.Hash(), r.mdf1HashAlg.Hash(), rand.Reader, (*rsa.PrivateKey)(privKey), data, []byte{})
	if err != nil {
		return nil, err
	}
	return decrypted, nil
}
