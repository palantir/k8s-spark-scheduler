// Copyright (c) 2018 Palantir Technologies. All rights reserved.
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
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
)

const (
	contentTypeFormURLEncoded = "application/x-www-form-urlencoded"
)

// FormURLEncoded (de)serializes Form parameters.
var FormURLEncoded Codec = codecFormURLEncoded{}

type codecFormURLEncoded struct{}

func (codecFormURLEncoded) Accept() string {
	return contentTypeFormURLEncoded
}

func (codecFormURLEncoded) Decode(r io.Reader, v interface{}) error {
	query, err := ioutil.ReadAll(r)
	if err != nil {
		return fmt.Errorf("failed to read all query bytes: %s", err.Error())
	}
	values, err := url.ParseQuery(string(query))
	if err != nil {
		return fmt.Errorf("failed to parse query: %s", err.Error())
	}
	dstValues, ok := v.(*url.Values)
	if !ok {
		return fmt.Errorf("could not decode, expected destination to be *url.Values, actual: %T", v)
	}
	*dstValues = values
	return nil
}

func (c codecFormURLEncoded) Unmarshal(data []byte, v interface{}) error {
	return c.Decode(bytes.NewBuffer(data), v)
}

func (codecFormURLEncoded) ContentType() string {
	return contentTypeFormURLEncoded
}

func (codecFormURLEncoded) Encode(w io.Writer, v interface{}) error {
	values, ok := v.(url.Values)
	if !ok {
		return fmt.Errorf("could not decode, expected destination to be url.Values, actual: %T", v)
	}
	encodedValue := values.Encode()
	_, err := w.Write([]byte(encodedValue))
	if err != nil {
		return fmt.Errorf("failed to write encoded url.Values to writer: %s", err.Error())
	}
	return nil
}

func (c codecFormURLEncoded) Marshal(v interface{}) ([]byte, error) {
	var buffer bytes.Buffer
	err := c.Encode(&buffer, v)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
