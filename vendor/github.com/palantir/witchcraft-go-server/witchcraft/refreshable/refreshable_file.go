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

package refreshable

import (
	"context"
	"crypto/sha256"
	"io/ioutil"
	"time"

	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	wparams "github.com/palantir/witchcraft-go-params"
)

type fileRefreshable struct {
	innerRefreshable *DefaultRefreshable

	filePath     string
	fileChecksum [sha256.Size]byte
}

const (
	defaultRefreshableSyncPeriod = time.Second
)

// NewFileRefreshable is identical to NewFileRefreshableWithDuration except it defaults to use defaultRefreshableSyncPeriod for how often the file is checked
func NewFileRefreshable(ctx context.Context, filePath string) (Refreshable, error) {
	return NewFileRefreshableWithDuration(ctx, filePath, defaultRefreshableSyncPeriod)
}

// NewFileRefreshableWithDuration returns a new Refreshable whose current value is the bytes of the file at the provided path.
// The file is checked every duration time.Duration as an argument.
// Calling this function also starts a goroutine which updates the value of the refreshable whenever the specified file
// is changed. The goroutine will terminate when the provided context is done or when the returned cancel function is
// called.
func NewFileRefreshableWithDuration(ctx context.Context, filePath string, duration time.Duration) (Refreshable, error) {
	initialBytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, werror.WrapWithContextParams(ctx, err, "failed to create file-based Refreshable because file could not be read", werror.SafeParam("filePath", filePath))
	}
	fRefreshable := &fileRefreshable{
		innerRefreshable: NewDefaultRefreshable(initialBytes),
		filePath:         filePath,
		fileChecksum:     sha256.Sum256(initialBytes),
	}
	fRefreshable.watchForChangesAsync(ctx, duration)
	return fRefreshable, nil
}

func (d *fileRefreshable) watchForChangesAsync(ctx context.Context, duration time.Duration) {
	go wapp.RunWithRecoveryLogging(wparams.ContextWithSafeParam(ctx, "filePath", d.filePath), func(ctx context.Context) {
		d.watchForChanges(ctx, duration)
	})
}

func (d *fileRefreshable) watchForChanges(ctx context.Context, duration time.Duration) {
	gcIntervalTicker := time.NewTicker(duration)
	defer gcIntervalTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-gcIntervalTicker.C:
			d.evaluateFileOnDisk(ctx)
		}
	}
}

func (d *fileRefreshable) evaluateFileOnDisk(ctx context.Context) {
	fileBytes, err := ioutil.ReadFile(d.filePath)
	if err != nil {
		svc1log.FromContext(ctx).Warn("Failed to read file bytes to update refreshable", svc1log.Stacktrace(err))
		return
	}
	loadedChecksum := sha256.Sum256(fileBytes)
	if loadedChecksum == d.fileChecksum {
		return
	}
	svc1log.FromContext(ctx).Debug("Attempting to update file refreshable")
	if err := d.innerRefreshable.Update(fileBytes); err != nil {
		svc1log.FromContext(ctx).Error("Failed to update refreshable with new file bytes", svc1log.Stacktrace(err))
		return
	}
	d.fileChecksum = loadedChecksum
}

func (d *fileRefreshable) Current() interface{} {
	return d.innerRefreshable.Current()
}

func (d *fileRefreshable) Subscribe(consumer func(interface{})) (unsubscribe func()) {
	return d.innerRefreshable.Subscribe(consumer)
}

func (d *fileRefreshable) Map(mapFn func(interface{}) interface{}) Refreshable {
	return d.innerRefreshable.Map(mapFn)
}
