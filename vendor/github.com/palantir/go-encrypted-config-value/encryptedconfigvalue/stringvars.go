// Copyright 2017 Palantir Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package encryptedconfigvalue

import (
	"fmt"
	"regexp"
	"strings"
)

const stringVarEncryptedValRegexpStr = `\${enc:[^}]+}`

var stringVarEncryptedValueRegexp = regexp.MustCompile(stringVarEncryptedValRegexpStr)

// StringVar represents a string variable. It is a string of the form "${...}", where the content of the string variable
// is the text that occurs between the braces.
type StringVar string

// ContainsEncryptedConfigValueStringVars returns true if the provided input contains any occurrences of string
// variables that consist of encrypted values. These are of the form "${enc:...}".
func ContainsEncryptedConfigValueStringVars(input []byte) bool {
	return stringVarEncryptedValueRegexp.Match(input)
}

// DecryptAllEncryptedValueStringVars returns a new byte slice that is based on the input but where all occurrences of
// string variables that consist of encrypted values are replaced with the value obtained when decrypting the encrypted
// value using the provided key.
func DecryptAllEncryptedValueStringVars(input []byte, key KeyWithType) []byte {
	return replaceAllStringVars(input, func(raw []byte) ([]byte, bool) {
		encryptedVal, err := NewEncryptedValue(string(raw))
		if err != nil {
			return raw, true
		}
		decrypted, err := encryptedVal.Decrypt(key)
		if err != nil {
			return raw, true
		}
		return []byte(decrypted), false
	})
}

// NormalizeEncryptedValueStringVars returns a new byte slice in which all of the string variables in the input that
// consist of encrypted values that have the same decrypted plaintext representation when decrypted using the provided
// key will be normalized such that their encrypted values are the same. The replacement is performed on the content of
// the string variables, so the returned slice will still contain string variables.
//
// If the decrypted plaintext exists as a key in the "normalized" map, then it is substituted with the value in that
// map. If the map does not contain an entry for the plaintext, the first time it is encountered it is added to the map
// with its corresponding EncryptedValue, and every subsequent occurrence in the input will use the normalized value. On
// completion of the function, the "normalized" map will contain a key for every plaintext value in the input where the
// value will be the EncryptedValue that was used for all occurrences.
//
// WARNING: after this function has been executed, the keys of the "normalized" map will contain all of the decrypted
// values in the input -- its use should be tracked carefully. The "normalized" version of the input is also less
// cryptographically secure because it makes the output more predictable -- for example, it makes it possible to
// determine that multiple different encrypted values have the same underlying plaintext value.
//
// The intended usage of this function is limited to very specific cases in which there is a requirement that the same
// plaintext must render to the same encrypted value for a specific key. Ensure that you fully understand the
// ramifications of this and only use this function if it is absolutely necessary.
func NormalizeEncryptedValueStringVars(input []byte, key KeyWithType, normalized map[string]EncryptedValue) []byte {
	return replaceAllStringVars(input, func(raw []byte) ([]byte, bool) {
		encryptedVal, err := NewEncryptedValue(string(raw))
		if err != nil {
			return raw, true
		}
		decrypted, err := encryptedVal.Decrypt(key)
		if err != nil {
			return raw, true
		}
		plaintext := decrypted
		// if an entry for the plaintext of the current encrypted value exists in the normalized map, replace
		// the encrypted value with the normalized one.
		if sub, present := normalized[plaintext]; present {
			return []byte(sub.ToSerializable()), true
		}
		// this is the first time that this plaintext has been encountered for an encrypted value. Store the
		// current encrypted value as the value for the plaintext in the map so that all subsequent occurrences
		// will use this encrypted value.
		normalized[plaintext] = encryptedVal
		return raw, true
	})
}

// DecryptSingleEncryptedValueStringVarString takes the content of a string variable, interprets it as an EncryptedValue
// and returns the result of decrypting that value using the provided key. Returns an error if the input string variable
// is not a valid string variable, if the contents of the string variable is not a valid EncryptedValue, or if the
// decryption fails.
func DecryptSingleEncryptedValueStringVarString(input string, key KeyWithType) (string, error) {
	return DecryptSingleEncryptedValueStringVar(StringVar(input), key)
}

// DecryptSingleEncryptedValueStringVar takes the content of a string variable, interprets it as an EncryptedValue and
// returns the result of decrypting that value using the provided key. Returns an error if the input string variable is
// not a valid string variable, if the contents of the string variable is not a valid EncryptedValue, or if the
// decryption fails.
func DecryptSingleEncryptedValueStringVar(input StringVar, key KeyWithType) (string, error) {
	contents, err := input.Contents()
	if err != nil {
		return "", err
	}
	ev, err := NewEncryptedValue(contents)
	if err != nil {
		return "", err
	}
	return ev.Decrypt(key)
}

// NewStringVar returns a new string variable that consists of wrapping the provided string within a string variable
// format. It does so by prepending "${" and appending "}" to the input. This string variable form is compatible with
// the default string variable form used by the org.apache.commons.lang3.text.StrSubstitutor library.
func NewStringVar(input string) StringVar {
	return StringVar(fmt.Sprintf(`${%s}`, input))
}

// Contents returns contents of the string variable. Returns an error if the provided input is not a valid string
// variable (if it does not start with "${" and end with "}").
func (sv StringVar) Contents() (string, error) {
	inputStr := string(sv)
	if !strings.HasPrefix(inputStr, `${`) && !strings.HasSuffix(inputStr, `}`) {
		return "", fmt.Errorf("string variable must be of the form %q, was: %s", `${...}`, inputStr)
	}
	return inputStr[len(`${`) : len(inputStr)-len(`}`)], nil
}

const stringVarRegexpConst = `\${([^}]+)}`

var stringVarRegxp = regexp.MustCompile(stringVarRegexpConst)

// stringVarReplacer receives the sequence of bytes that is the content of a string variable of the default form specified
// by the org.apache.commons.lang3.text.StrSubstitutor library (for an input like "${...}", the content of the braces)
// and returns the sequence of bytes that should replace the variable. The bool indicates whether the returned bytes
// should replace the entire variable or just the contents. For example, given "${foo}", a replacer that returns
// ("bar", false) would result in "bar", while a replacer that returns ("bar", true) would result in "${bar}".
type stringVarReplacer func([]byte) (replacement []byte, replaceVarContentOnly bool)

// replaceAllStringVars finds all occurrences of string variables for the form "${...}" and performs replacement on them
// using the provided replacement function. The return value of the replacement function controls whether the
// replacement is done on the content of the variable or for the entire variable string.
func replaceAllStringVars(input []byte, replaceContent stringVarReplacer) []byte {
	return stringVarRegxp.ReplaceAllFunc(input, func(raw []byte) []byte {
		content, replaceContentOnly := replaceContent(raw[len([]byte("${")) : len(raw)-len([]byte("}"))])
		if replaceContentOnly {
			content = []byte("${" + string(content) + "}")
		}
		return content
	})
}
