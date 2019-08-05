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

package store

import (
	"hash/fnv"
	"sync"
)

const (
	// asyncRequestBufferSize is the maximum number of requests
	// to be queued, after this number requests to add more elements
	// will start to block callers.
	asyncRequestBufferSize = 100
)

// ShardedUniqueQueue is a queue of write requests
// for objects. It compacts consecutive create and update
// requests, provides a slice of channels for consumers.
// No two requests for the same object will end up in
// different consumers.
type ShardedUniqueQueue interface {
	AddIfAbsent(Request)
	GetConsumers() []<-chan func() Request
	QueueLengths() []int
}

// NewShardedUniqueQueue creates a sharded queue of write requests
func NewShardedUniqueQueue(buckets int) ShardedUniqueQueue {
	queues := make([]chan func() Request, 0, buckets)
	for i := 0; i < buckets; i++ {
		queues = append(queues, make(chan func() Request, asyncRequestBufferSize))
	}
	return &shardedUniqueQueue{
		queues:   queues,
		inflight: make(map[Key]bool),
	}
}

type shardedUniqueQueue struct {
	queues   []chan func() Request
	inflight map[Key]bool
	lock     sync.Mutex
}

// AddIfAbsent puts a request to the queue if it is absent,
// or if it is a delete request. Deletes are not compacted
// to prior update or create requests, so all input objects
// can be created
func (q *shardedUniqueQueue) AddIfAbsent(r Request) {
	added := q.addToInflightIfAbsent(r.Key)
	if added || r.Type == DeleteRequestType {
		q.queues[q.bucket(r.Key)] <- func() Request {
			q.deleteFromStore(r.Key)
			return r
		}
	}
}

// GetConsumers returns a slice of receive only channels for consumers.
// requests for the same object will always end up in the same consumer.
func (q *shardedUniqueQueue) GetConsumers() []<-chan func() Request {
	res := make([]<-chan func() Request, 0, len(q.queues))
	for _, queue := range q.queues {
		res = append(res, queue)
	}
	return res
}

func (q *shardedUniqueQueue) QueueLengths() []int {
	res := make([]int, 0, len(q.queues))
	for _, c := range q.queues {
		res = append(res, len(c))
	}
	return res
}

func (q *shardedUniqueQueue) bucket(k Key) uint32 {
	h := fnv.New32a()
	h.Write([]byte(k.Namespace))
	h.Write([]byte(k.Name))
	return h.Sum32() % uint32(len(q.queues))
}

func (q *shardedUniqueQueue) addToInflightIfAbsent(k Key) bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	if _, ok := q.inflight[k]; ok {
		return false
	}
	q.inflight[k] = true
	return true
}

func (q *shardedUniqueQueue) deleteFromStore(k Key) {
	q.lock.Lock()
	defer q.lock.Unlock()
	delete(q.inflight, k)
}
