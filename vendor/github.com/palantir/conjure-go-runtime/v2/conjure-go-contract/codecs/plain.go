// Copyright (c) 2019 Palantir Technologies. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package codecs

import (
	"encoding"
	"io"
	"io/ioutil"
	"reflect"

	werror "github.com/palantir/witchcraft-go-error"
)

const (
	contentTypePlain = "text/plain"
)

// Plain implements a text/plain codec. Values used to marshal/unmarshal
// must be of type string or encoding.TextMarshaler/encoding.TextUnmarshaler.
var Plain Codec = codecPlain{}

type codecPlain struct{}

func (codecPlain) Accept() string {
	return contentTypePlain
}

func (codecPlain) Decode(r io.Reader, v interface{}) error {
	text, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	return Plain.Unmarshal(text, v)
}

func (codecPlain) Unmarshal(data []byte, v interface{}) error {
	switch t := v.(type) {
	case *string:
		*t = string(data)
		return nil
	case encoding.TextUnmarshaler:
		return t.UnmarshalText(data)
	}
	return werror.Error("unmarshal target must be a string or encoding.TextUnmarshaler", werror.SafeParam("type", reflect.TypeOf(v).String()))
}

func (codecPlain) ContentType() string {
	return contentTypePlain
}

func (codecPlain) Encode(w io.Writer, v interface{}) error {
	data, err := Plain.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func (codecPlain) Marshal(v interface{}) ([]byte, error) {
	switch t := v.(type) {
	case string:
		return []byte(t), nil
	case encoding.TextMarshaler:
		return t.MarshalText()
	default:
		val := reflect.ValueOf(v)
		if val.Kind() == reflect.Ptr {
			return Plain.Marshal(val.Elem().Interface())
		}
	}
	return nil, werror.Error("marshal target must be a string or encoding.TextMarshaler", werror.SafeParam("type", reflect.TypeOf(v).String()))
}
