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

import (
	"net/http"

	"github.com/palantir/witchcraft-go-tracing/wtracing"
)

// SpanInjector returns a SpanInjector that injects a wtracing.SpanContext in the header of the provided *http.Request.
// The injector will only set a TraceID and SpanID if both values are non-empty, and will also only set a ParentID if it
// is non-empty and the TraceID and SpanID are also non-empty. If the provided span is in debug mode, the flags header
// will be set, but the sampled header will not be. If the provided span is not in debug mode, then the sampled header
// will explicitly be set to "0" or "1".
func SpanInjector(req *http.Request) wtracing.SpanInjector {
	return func(sc wtracing.SpanContext) {
		if len(sc.TraceID) > 0 && len(sc.ID) > 0 {
			req.Header.Set(b3TraceID, string(sc.TraceID))
			req.Header.Set(b3SpanID, string(sc.ID))
			if parentID := sc.ParentID; parentID != nil {
				req.Header.Set(b3ParentSpanID, string(*sc.ParentID))
			}
		}

		if sc.Debug {
			req.Header.Set(b3Flags, trueHeaderVal)
		} else if sampled := sc.Sampled; sampled != nil {
			sampledVal := falseHeaderVal
			if *sampled {
				sampledVal = trueHeaderVal
			}
			req.Header.Set(b3Sampled, sampledVal)
		}
	}
}
