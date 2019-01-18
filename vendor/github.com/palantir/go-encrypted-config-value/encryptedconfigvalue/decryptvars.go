// Copyright 2017 Palantir Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package encryptedconfigvalue

import (
	"reflect"
)

// DecryptEncryptedStringVariables decrypts any encrypted string variables (strings of the form "${enc:<...>}" in the
// provided object in-place using the provided key. The input value should be a pointer so that the modifications are
// visible to the caller.
//
// This function recursively examines all string values in the exported fields of the provided input, searches for
// encrypted string variables within them, and replaces all such variables with their decrypted values if decryption is
// successful using the provided key. String values are string fields, string pointers and strings in slices, arrays, or
// values of maps. Map keys are specifically not included.
func DecryptEncryptedStringVariables(in interface{}, key KeyWithType) {
	decryptEncryptedStringVariables(reflect.ValueOf(in), key)
}

// CopyWithEncryptedStringVariablesDecrypted copies the provided input and returns the result of calling
// DecryptEncryptedStringVariables on the copy. The input value is left unmodified (however, if the input value is a
// pointer or contains pointers, those values may be modified, as the initial copy of the input that is made is a
// shallow copy rather than a deep one). The returned value will be the same type as the value that was provided.
func CopyWithEncryptedStringVariablesDecrypted(in interface{}, key KeyWithType) interface{} {
	newVal := reflect.New(reflect.ValueOf(in).Type()).Elem()
	newVal.Set(reflect.ValueOf(in))

	out := newVal.Interface()
	DecryptEncryptedStringVariables(&out, key)
	return out
}

func decryptEncryptedStringVariables(v reflect.Value, key KeyWithType) {
	switch v.Kind() {
	case reflect.String:
		if !v.CanSet() {
			return
		}
		currStrBytes := []byte(v.String())
		if ContainsEncryptedConfigValueStringVars(currStrBytes) {
			decryptedVal := reflect.ValueOf(string(DecryptAllEncryptedValueStringVars(currStrBytes, key)))
			// convert value to destination type to handle cases in which the destination type is a type alias for a string
			decryptedVal = decryptedVal.Convert(v.Type())
			v.Set(decryptedVal)
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			decryptEncryptedStringVariables(v.Field(i), key)
		}
	case reflect.Array, reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			decryptEncryptedStringVariables(v.Index(i), key)
		}
	case reflect.Map:
		for _, k := range v.MapKeys() {
			newVal := reflect.New(v.MapIndex(k).Type()).Elem()
			newVal.Set(v.MapIndex(k))
			decryptEncryptedStringVariables(newVal, key)
			v.SetMapIndex(k, newVal)
		}
	case reflect.Interface:
		if !v.CanSet() || v.IsNil() {
			return
		}

		// instantiate new value that is a copy of the original
		newVal := reflect.New(v.Elem().Type()).Elem()
		newVal.Set(v.Elem())

		decryptEncryptedStringVariables(newVal, key)
		v.Set(newVal)
	case reflect.Ptr:
		if v.IsNil() {
			return
		}
		decryptEncryptedStringVariables(v.Elem(), key)
	}
}
