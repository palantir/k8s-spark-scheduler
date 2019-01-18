// Copyright 2017 Palantir Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package encryption

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"fmt"
)

// NewRSAKeyPair creates an RSA key pair of the provided size and returns the public and private keys for the pair.
func NewRSAKeyPair(keySizeBits int) (pubKey *RSAPublicKey, privKey *RSAPrivateKey, err error) {
	rsaPrivKey, err := rsa.GenerateKey(rand.Reader, keySizeBits)
	if err != nil {
		return nil, nil, err
	}
	return rsaPublicKeyFromKey(&rsaPrivKey.PublicKey), rsaPrivateKeyFromKey(rsaPrivKey), nil
}

// RSAPublicKey is an RSA public key that can be used for RSA encryption operations.
type RSAPublicKey rsa.PublicKey

// Bytes returns the PEM representation of this public key.
func (r *RSAPublicKey) Bytes() []byte {
	asn1PubKey, err := x509.MarshalPKIXPublicKey((*rsa.PublicKey)(r))
	if err != nil {
		// should never occur for valid key
		panic(err)
	}
	return pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PUBLIC KEY",
		Bytes: asn1PubKey,
	})
}

// RSAPublicKeyFromPEMBytes returns a new RSA public key using the provided bytes, which should be the PEM
// representation of the public key.
func RSAPublicKeyFromPEMBytes(key []byte) (*RSAPublicKey, error) {
	var errInvalidRSAPublicKeyError = fmt.Errorf("key is not a valid PEM-encoded RSA public key")

	block, _ := pem.Decode(key)
	if block == nil {
		return nil, errInvalidRSAPublicKeyError
	}
	pkixPubKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, errInvalidRSAPublicKeyError
	}
	rsaPubKey, ok := pkixPubKey.(*rsa.PublicKey)
	if !ok {
		return nil, errInvalidRSAPublicKeyError
	}
	return rsaPublicKeyFromKey(rsaPubKey), nil
}

func rsaPublicKeyFromKey(rsaPubKey *rsa.PublicKey) *RSAPublicKey {
	return (*RSAPublicKey)(rsaPubKey)
}

// RSAPrivateKey is an RSA private key that can be used for RSA decryption operations.
type RSAPrivateKey rsa.PrivateKey

// Bytes returns the PKCS#8 representation of this private key.
func (r *RSAPrivateKey) Bytes() []byte {
	pkey := struct {
		Version             int
		PrivateKeyAlgorithm []asn1.ObjectIdentifier
		PrivateKey          []byte
	}{
		Version:             0,
		PrivateKeyAlgorithm: make([]asn1.ObjectIdentifier, 1),
	}
	// OID of RSA public key for PKCS#8 1.2.840.113549.1.1.1: see "Public Key file (PKCS#8)"
	// section of https://tls.mbed.org/kb/cryptography/asn1-key-structures-in-der-and-pem
	pkey.PrivateKeyAlgorithm[0] = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 1, 1}
	pkey.PrivateKey = x509.MarshalPKCS1PrivateKey((*rsa.PrivateKey)(r))
	bytes, err := asn1.Marshal(pkey)
	if err != nil {
		// should never occur for valid key
		panic(err)
	}
	return bytes
}

// RSAPrivateKeyFromPKCS8Bytes returns a new RSA private key using the provided bytes, which should be the PKCS#8
// representation of the private key.
func RSAPrivateKeyFromPKCS8Bytes(key []byte) (*RSAPrivateKey, error) {
	pkcsPrivKey, err := x509.ParsePKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("invalid PKCS8 private key: %v", err)
	}
	rsaPrivKey, ok := pkcsPrivKey.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("invalid PKCS8 private key: %v", err)
	}
	return rsaPrivateKeyFromKey(rsaPrivKey), nil
}

func rsaPrivateKeyFromKey(rsaPrivKey *rsa.PrivateKey) *RSAPrivateKey {
	return (*RSAPrivateKey)(rsaPrivKey)
}
