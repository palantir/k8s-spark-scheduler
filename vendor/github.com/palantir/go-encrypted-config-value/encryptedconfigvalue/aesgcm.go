// Copyright 2017 Palantir Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package encryptedconfigvalue

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/palantir/go-encrypted-config-value/encryption"
)

const (
	aesGCMLegacyNonceSizeBytes = 32
	aesGCMLegacyTagSizeBytes   = 16
)

type legacyAESGCMEncrypter encryption.AESGCMCipher

// LegacyAESGCMEncrypter returns an encrypter that encrypts values using the legacy AES parameters (256-bit nonce and
// 128-bit tag). The returned EncryptedValue will also be serialized in the legacy format of "AES:<base64-encoded-(nonce+ciphertext+tag)>".
func LegacyAESGCMEncrypter() Encrypter {
	return (*legacyAESGCMEncrypter)(encryption.AESGCMCipherWithNonceAndTagSize(aesGCMLegacyNonceSizeBytes, aesGCMLegacyTagSizeBytes))
}

func (a *legacyAESGCMEncrypter) Encrypt(input string, key KeyWithType) (EncryptedValue, error) {
	aesGCMCipher := (*encryption.AESGCMCipher)(a)

	// encryptedBytes consists of [nonce + encrypted + tag]
	encryptedBytes, err := aesGCMCipher.Encrypt([]byte(input), key.Key)
	if err != nil {
		return nil, err
	}

	return &legacyEncryptedValue{
		encryptedBytes: encryptedBytes,
	}, nil
}

const (
	aesGCMDefaultNonceSizeBytes = 12
	aesGCMDefaultTagSizeBytes   = 16
)

type aesGCMEncrypter encryption.AESGCMCipher

// NewAESGCMEncrypter returns an encrypter that encrypts values using encrypted-config-value's standard AES parameters
// (96-bit nonce and 128-bit tag). The returned EncryptedValue will be serialized in the new format of "AES:<base64-encoded-JSON>",
// where the JSON is the JSON representation of the aesGCMEncryptedValueJSON struct.
func NewAESGCMEncrypter() Encrypter {
	return (*aesGCMEncrypter)(encryption.AESGCMCipherWithNonceAndTagSize(aesGCMDefaultNonceSizeBytes, aesGCMDefaultTagSizeBytes))
}

func (a *aesGCMEncrypter) Encrypt(input string, key KeyWithType) (EncryptedValue, error) {
	aesGCMCipher := (*encryption.AESGCMCipher)(a)

	// encryptedBytes consists of [nonce + encrypted + tag]
	encryptedBytes, err := aesGCMCipher.Encrypt([]byte(input), key.Key)
	if err != nil {
		return nil, err
	}

	nonce, encrypted, tag := aesGCMCipher.Parts(encryptedBytes)

	return &aesGCMEncryptedValue{
		encrypted: encrypted,
		nonce:     nonce,
		tag:       tag,
	}, nil
}

type aesGCMEncryptedValue struct {
	encrypted []byte
	nonce     []byte
	tag       []byte
}

type aesGCMEncryptedValueJSON struct {
	Type       string `json:"type"`
	Mode       string `json:"mode"`
	Ciphertext string `json:"ciphertext"`
	IV         string `json:"iv"`
	Tag        string `json:"tag"`
}

const gcmMode = "GCM"

func (ev aesGCMEncryptedValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(aesGCMEncryptedValueJSON{
		Type:       string(AES),
		Mode:       gcmMode,
		Ciphertext: base64.StdEncoding.EncodeToString(ev.encrypted),
		IV:         base64.StdEncoding.EncodeToString(ev.nonce),
		Tag:        base64.StdEncoding.EncodeToString(ev.tag),
	})
}

func (ev *aesGCMEncryptedValue) UnmarshalJSON(data []byte) error {
	var evJSON aesGCMEncryptedValueJSON
	if err := json.Unmarshal(data, &evJSON); err != nil {
		return err
	}
	if evJSON.Mode != gcmMode {
		return fmt.Errorf("unsupported mode: only %q mode is supported for AES, but was %q", gcmMode, evJSON.Mode)
	}

	encrypted, err := base64.StdEncoding.DecodeString(evJSON.Ciphertext)
	if err != nil {
		return err
	}
	nonce, err := base64.StdEncoding.DecodeString(evJSON.IV)
	if err != nil {
		return err
	}
	tag, err := base64.StdEncoding.DecodeString(evJSON.Tag)
	if err != nil {
		return err
	}
	*ev = aesGCMEncryptedValue{
		encrypted: encrypted,
		nonce:     nonce,
		tag:       tag,
	}
	return nil
}

func (ev *aesGCMEncryptedValue) Decrypt(key KeyWithType) (string, error) {
	aesGCMCipher := encryption.AESGCMCipherWithNonceAndTagSize(len(ev.nonce), len(ev.tag))
	encrypted := append(ev.nonce, append(ev.encrypted, ev.tag...)...)
	decrypted, err := aesGCMCipher.Decrypt(encrypted, key.Key)
	return string(decrypted), err
}

func (ev *aesGCMEncryptedValue) ToSerializable() SerializedEncryptedValue {
	return encryptedValToSerializable(ev)
}
