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

package b3

const (
	b3TraceID      = "X-B3-TraceId"
	b3SpanID       = "X-B3-SpanId"
	b3ParentSpanID = "X-B3-ParentSpanId"
	b3Sampled      = "X-B3-Sampled"
	b3Flags        = "X-B3-Flags"

	falseHeaderVal = "0"
	trueHeaderVal  = "1"
)
