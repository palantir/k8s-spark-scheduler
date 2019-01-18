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

package wapp

import (
	"context"
	"runtime/debug"

	"github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/diaglog/diag1log"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
)

// RunWithFatalLogging wraps a callback, logging errors and panics it returns.
// Useful as a "catch all" for applications so that they can sls log fatal events, perhaps before exiting.
func RunWithFatalLogging(ctx context.Context, runFn func(ctx context.Context) error) (retErr error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		stacktrace := diag1log.ThreadDumpV1FromGoroutines(debug.Stack())
		svc1log.FromContext(ctx).Error("panic recovered",
			svc1log.SafeParam("stacktrace", stacktrace),
			svc1log.UnsafeParam("recovered", r))
		if retErr == nil {
			retErr = werror.Error("panic recovered",
				werror.SafeParam("stacktrace", stacktrace),
				werror.UnsafeParam("recovered", r))
		}
	}()
	if err := runFn(ctx); err != nil {
		svc1log.FromContext(ctx).Error("error", svc1log.Stacktrace(err))
		return err
	}
	return nil
}
