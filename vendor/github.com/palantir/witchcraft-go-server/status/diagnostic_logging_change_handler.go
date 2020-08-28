// Copyright (c) 2020 Palantir Technologies. All rights reserved.
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

package status

import (
	"bytes"
	"context"
	"runtime/pprof"

	"github.com/palantir/witchcraft-go-logging/conjure/witchcraft/api/logging"
	"github.com/palantir/witchcraft-go-logging/wlog/diaglog/diag1log"
	"github.com/palantir/witchcraft-go-server/conjure/witchcraft/api/health"
)

// NewDiagnosticLoggingChangeHandler will emit a diagnostic log whenever the latest health status' state is more severe
// than HealthStateRepairing.
func NewDiagnosticLoggingChangeHandler() HealthStatusChangeHandler {
	return healthStatusChangeHandlerFn(func(ctx context.Context, prev, curr health.HealthStatus) {
		if HealthStatusCode(curr) > HealthStateStatusCodes[health.HealthStateRepairing] {
			var buf bytes.Buffer
			_ = pprof.Lookup("goroutine").WriteTo(&buf, 2) // bytes.Buffer's Write never returns an error, so we swallow it
			diag1log.FromContext(ctx).Diagnostic(logging.NewDiagnosticFromThreadDump(diag1log.ThreadDumpV1FromGoroutines(buf.Bytes())))
		}
	})
}
