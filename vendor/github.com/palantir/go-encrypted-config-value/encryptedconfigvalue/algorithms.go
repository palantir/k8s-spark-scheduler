// Copyright 2017 Palantir Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package encryptedconfigvalue

import (
	"fmt"

	"github.com/palantir/go-encrypted-config-value/encryption"
)

// AlgorithmType represents the algorithm used to encrypt a value.
type AlgorithmType string

const (
	AES = AlgorithmType("AES")
	RSA = AlgorithmType("RSA")
)

type keyPairGenerator func() (KeyPair, error)

type algorithmTypeData struct {
	generator keyPairGenerator
	encrypter Encrypter
}

var algorithmTypeToData = map[AlgorithmType]algorithmTypeData{
	AES: {
		generator: NewAESKeyPair,
		encrypter: NewAESGCMEncrypter(),
	},
	RSA: {
		generator: NewRSAKeyPair,
		encrypter: NewRSAOAEPEncrypter(),
	},
}

// GenerateKeyPair generates a new KeyPair using the default size/parameters specified by encrypted-config-value that
// can be used to encrypt and decrypt values for the receiver algorithm.
func (a AlgorithmType) GenerateKeyPair() (KeyPair, error) {
	return algorithmTypeToData[a].generator()
}

// Encrypter returns a new Encrypter that uses the default encryption parameters specified by encrypted-config-value
// that can be used to create EncryptedValue objects for the receiver algorithm.
func (a AlgorithmType) Encrypter() Encrypter {
	return algorithmTypeToData[a].encrypter
}

// ToAlgorithmType returns the AlgorithmType that matches the provided string. Returns an error if the provided string
// does not match a known algorithm.
func ToAlgorithmType(val string) (AlgorithmType, error) {
	algType := AlgorithmType(val)
	if _, ok := algorithmTypeToData[algType]; !ok {
		return AlgorithmType(""), fmt.Errorf("unknown algorithm type: %q", val)
	}
	return algType, nil
}

// KeyType represents a specific type of key.
type KeyType string

const (
	AESKey     = KeyType("AES")
	RSAPubKey  = KeyType("RSA-PUB")
	RSAPrivKey = KeyType("RSA-PRIV")
)

type keyTypeData struct {
	generator KeyGenerator
	algType   AlgorithmType
}

var keyTypeToData = map[KeyType]keyTypeData{
	AESKey: {
		generator: keyGeneratorFor(AESKey, func(key []byte) (encryption.Key, error) {
			return encryption.AESKeyFromBytes(key), nil
		}),
		algType: AES,
	},
	RSAPubKey: {
		generator: keyGeneratorFor(RSAPubKey, func(key []byte) (encryption.Key, error) {
			return encryption.RSAPublicKeyFromPEMBytes(key)
		}),
		algType: RSA,
	},
	RSAPrivKey: {
		generator: keyGeneratorFor(RSAPrivKey, func(key []byte) (encryption.Key, error) {
			return encryption.RSAPrivateKeyFromPKCS8Bytes(key)
		}),
		algType: RSA,
	},
}

// Generator returns a new KeyGenerator which, given the byte representation for the content of a key of the receiver
// type, returns a new KeyWithType for a key of the receiver type.
func (kt KeyType) Generator() KeyGenerator {
	return keyTypeToData[kt].generator
}

// AlgorithmType returns the encryption algorithm that corresponds the the key type of the receiver.
func (kt KeyType) AlgorithmType() AlgorithmType {
	return keyTypeToData[kt].algType
}

// KeyGenerator defines a function which, given the byte representation of a key, returns a KeyWithType. The provided
// bytes are typically the raw or encoded bytes for the key itself. It is typically the responsibility of the generator
// function to provide the KeyType information required for the returned KeyWithType.
type KeyGenerator func([]byte) (KeyWithType, error)

func keyGeneratorFor(keyType KeyType, keyGen func([]byte) (encryption.Key, error)) KeyGenerator {
	return func(keyBytes []byte) (KeyWithType, error) {
		key, err := keyGen(keyBytes)
		if err != nil {
			return KeyWithType{}, err
		}
		return KeyWithType{
			Type: keyType,
			Key:  key,
		}, nil
	}
}

// ToKeyType returns the KeyType that matches the provided string. Returns an error if the provided string does not
// match a known key type.
func ToKeyType(val string) (KeyType, error) {
	keyType := KeyType(val)
	if _, ok := keyTypeToData[keyType]; !ok {
		return KeyType(""), fmt.Errorf("unknown key type: %q", val)
	}
	return keyType, nil
}
