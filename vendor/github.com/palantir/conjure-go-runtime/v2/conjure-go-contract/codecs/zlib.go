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
	"compress/zlib"
	"fmt"
	"io"
)

var _ Codec = codecZLIB{}

// ZLIB wraps an existing Codec and uses zlib for compression and decompression.
func ZLIB(codec Codec) Codec {
	return &codecZLIB{contentCodec: codec}
}

type codecZLIB struct {
	contentCodec Codec
}

func (c codecZLIB) Accept() string {
	return c.contentCodec.Accept()
}

func (c codecZLIB) Decode(r io.Reader, v interface{}) error {
	zlibReader, err := zlib.NewReader(r)
	if err != nil {
		return fmt.Errorf("failed to create zlib reader: %s", err.Error())
	}
	return c.contentCodec.Decode(zlibReader, v)
}

func (c codecZLIB) Unmarshal(data []byte, v interface{}) error {
	return c.Decode(bytes.NewBuffer(data), v)
}

func (c codecZLIB) ContentType() string {
	return c.contentCodec.ContentType()
}

func (c codecZLIB) Encode(w io.Writer, v interface{}) (err error) {
	zlibWriter := zlib.NewWriter(w)
	defer func() {
		if closeErr := zlibWriter.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	return c.contentCodec.Encode(zlibWriter, v)
}

func (c codecZLIB) Marshal(v interface{}) ([]byte, error) {
	var buffer bytes.Buffer
	err := c.Encode(&buffer, v)
	if err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte{'\n'}), nil
}
