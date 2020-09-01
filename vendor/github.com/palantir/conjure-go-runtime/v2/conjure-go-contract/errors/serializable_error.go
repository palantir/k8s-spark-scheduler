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

package errors

import (
	"encoding/json"

	"github.com/palantir/pkg/uuid"
)

// SerializableError is serializable representation of an error, it includes error code, name, instance id
// and parameters. It can be used to implement error marshaling & unmarshaling of concrete
// types implementing an Error interface.
//
// This type does not marshal & unmarshal parameters - that should be
// responsibility of a type implementing an Error.
//
// This is an example of a valid JSON object representing an error:
//
//  {
//    "errorCode": "CONFLICT",
//    "errorName": "Facebook:LikeAlreadyGiven",
//    "errorInstanceId": "00010203-0405-0607-0809-0a0b0c0d0e0f",
//    "parameters": {
//      "postId": "5aa734gs3579",
//      "userId": 642764872364
//    }
//  }
type SerializableError struct {
	ErrorCode       ErrorCode       `json:"errorCode"`
	ErrorName       string          `json:"errorName"`
	ErrorInstanceID uuid.UUID       `json:"errorInstanceId"`
	Parameters      json.RawMessage `json:"parameters,omitempty"`
}
