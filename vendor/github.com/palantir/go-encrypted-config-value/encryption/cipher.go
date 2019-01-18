// Copyright 2017 Palantir Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package encryption

type Cipher interface {
	// Encrypt encrypts the provided data using the provided key. The provided key must be capable of encrypting
	// values for this cipher. The specific nature of the output bytes depends on the algorithm of the cipher (for
	// example, for an AES cipher, the returned bytes may include the nonce and tag in addition to the raw
	// ciphertext). Refer to the concrete implementation of the cipher for information on how the format of the
	// returned bytes. Returns an error if the provided key cannot be used to encrypt values for this cipher or if
	// an error is encountered during encryption.
	Encrypt(data []byte, key Key) ([]byte, error)

	// Decrypt decrypts the provided data using the provided key. The provided key must be capable of decrypting
	// values for this cipher. The input should be the output of an Encrypt operation for this cipher. Returns an
	// error if the provided key cannot be used to encrypt values for this cipher or if an error is encountered
	// during encryption.
	Decrypt(data []byte, key Key) ([]byte, error)
}

// Key represents a key that can be used for encryption or decryption operations. This is used as a marker interface --
// structs that implement this interface will typically have more structured key information, and implementations of
// Cipher will assert that a Key is of a particular type before proceeding.
type Key interface {
	// Bytes returns the byte representation of this key. Refer to the concrete implementation for information on
	// the exact format of the returned bytes.
	Bytes() []byte
}
