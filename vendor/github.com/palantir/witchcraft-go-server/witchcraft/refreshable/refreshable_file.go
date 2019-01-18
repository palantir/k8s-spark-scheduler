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
	"path/filepath"

	"github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"gopkg.in/fsnotify.v1"
)

type fileRefreshable struct {
	innerRefreshable *DefaultRefreshable

	filePath     string
	fileChecksum [sha256.Size]byte
}

// NewFileRefreshable returns a new Refreshable whose current value is the bytes of the file at the provided path.
// Calling this function also starts a goroutine which updates the value of the refreshable whenever the specified file
// is changed. The goroutine will terminate when the provided context is done or when the returned cancel function is
// called.
func NewFileRefreshable(ctx context.Context, filePath string) (r Refreshable, rErr error) {
	initialBytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, werror.Wrap(err, "failed to create file-based Refreshable because file could not be read", werror.SafeParam("filePath", filePath))
	}
	fRefreshable := &fileRefreshable{
		innerRefreshable: NewDefaultRefreshable(initialBytes),
		filePath:         filePath,
		fileChecksum:     sha256.Sum256(initialBytes),
	}
	if err := fRefreshable.watchForChanges(ctx); err != nil {
		return nil, err
	}
	return fRefreshable, nil
}

func (d *fileRefreshable) watchForChanges(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return werror.Wrap(err, "Failed to initialize file watcher")
	}
	// watch the directory to handle scenarios where the file is removed and then re-added
	fileDir := filepath.Dir(d.filePath)
	// use the base name of the fileLocation to normalize to the relative path the watcher will return
	relativeFilePath := filepath.Base(d.filePath)
	if err := watcher.Add(fileDir); err != nil {
		return werror.Wrap(err, "Failed to add file to watcher",
			werror.SafeParam("file", d.filePath),
			werror.SafeParam("fileDir", fileDir))
	}

	go func() {
		ctx := svc1log.WithLoggerParams(ctx, svc1log.SafeParam("filePath", d.filePath))
		for {
			select {
			case event := <-watcher.Events:
				// the watch is for the directory, so discard anything that doesn't match the file we want
				if filepath.Base(event.Name) != relativeFilePath {
					continue
				}
				// certain editors and write modes will create a new file and update it, this is why we are checking all
				// 3 operations
				if event.Op == fsnotify.Write || event.Op == fsnotify.Create || event.Op == fsnotify.Rename {
					fileBytes, err := ioutil.ReadFile(d.filePath)
					if err != nil {
						svc1log.FromContext(ctx).Warn("Failed to read file bytes to update refreshable")
						continue
					}
					loadedChecksum := sha256.Sum256(fileBytes)
					if loadedChecksum == d.fileChecksum {
						svc1log.FromContext(ctx).Debug("Ignoring fsnotify event for runtime configuration file because file checksum was unchanged")
						continue
					}
					if err := d.innerRefreshable.Update(fileBytes); err != nil {
						svc1log.FromContext(ctx).Error("Failed to update refreshable with new file bytes", svc1log.Stacktrace(err))
						continue
					}
					d.fileChecksum = loadedChecksum
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
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
