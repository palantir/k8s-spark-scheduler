// Copyright 2017 Palantir Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package encryptedconfigvalue

import (
	"github.com/palantir/go-encrypted-config-value/encryption"
)

// NewRSAKeys creates a new RSA public/private key pair of the specified size and returns new KeyWithType objects that
// is typed as an RSA public key and RSA private key and contains the corresponding key material.
func NewRSAKeys(keySizeBits int) (pubKey KeyWithType, privKey KeyWithType, err error) {
	pub, priv, err := encryption.NewRSAKeyPair(keySizeBits)
	if err != nil {
		return KeyWithType{}, KeyWithType{}, err
	}
	return KeyWithType{
			Type: RSAPubKey,
			Key:  pub,
		}, KeyWithType{
			Type: RSAPrivKey,
			Key:  priv,
		}, nil
}

// RSAPublicKeyFromBytes creates a new KeyWithType that contains an RSA public key constructed using the provided bytes.
// The input bytes must be the bytes for an X.509 PEM PKIX public key.
func RSAPublicKeyFromBytes(key []byte) (KeyWithType, error) {
	rsaPubKey, err := encryption.RSAPublicKeyFromPEMBytes(key)
	if err != nil {
		return KeyWithType{}, err
	}
	return RSAPublicKeyFromKey(rsaPubKey), nil
}

// RSAPublicKeyFromKey returns a new KeyWithType that wraps the provided RSAPublicKey.
func RSAPublicKeyFromKey(key *encryption.RSAPublicKey) KeyWithType {
	return KeyWithType{
		Type: RSAPubKey,
		Key:  key,
	}
}

// RSAPrivateKeyFromBytes creates a new KeyWithType that contains an RSA private key constructed using the provided bytes.
// The input bytes must be the bytes for an X.509 PKCS-8 private key.
func RSAPrivateKeyFromBytes(key []byte) (KeyWithType, error) {
	rsaPrivKey, err := encryption.RSAPrivateKeyFromPKCS8Bytes(key)
	if err != nil {
		return KeyWithType{}, err
	}
	return RSAPrivateKeyFromKey(rsaPrivKey), nil
}

// RSAPrivateKeyFromKey returns a new KeyWithType that wraps the provided RSAPrivateKey.
func RSAPrivateKeyFromKey(key *encryption.RSAPrivateKey) KeyWithType {
	return KeyWithType{
		Type: RSAPrivKey,
		Key:  key,
	}
}

const defaultRSAKeySizeBits = 2048

// NewRSAKeyPair creates a new KeyPair that contains a newly generated RSA key pair of the default size for encrypted-config-value
// (2048 bits). The encryption key of the key pair is the public key and the decryption key is the private key.
func NewRSAKeyPair() (KeyPair, error) {
	pubKey, privKey, err := NewRSAKeys(defaultRSAKeySizeBits)
	if err != nil {
		return KeyPair{}, err
	}
	return KeyPair{
		EncryptionKey: pubKey,
		DecryptionKey: privKey,
	}, nil
}
