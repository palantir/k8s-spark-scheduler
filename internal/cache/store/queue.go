package store

import (
	"hash/fnv"
	"sync"
)

type RequestType int

const (
	CreateRequest RequestType = 0
	UpdateRequest RequestType = 1
	DeleteRequest RequestType = 2
)

type Request struct {
	Key  Key
	Type RequestType
}

type ShardedUniqueQueue interface {
	AddIfAbsent(Request)
	GetConsumers() []<-chan func() Request
}

type shardedUniqueQueue struct {
	queues   []chan func() Request
	inflight map[Key]bool
	lock     sync.Mutex
}

// NewModifiableQueue creates a bucketed queue with modifiable
// items.
func NewShardedUniqueQueue(buckets int) ShardedUniqueQueue {
	queues := make([]chan func() Request, 0, buckets)
	for i := 0; i < buckets; i++ {
		queues = append(queues, make(chan func() Request, 100))
	}
	return &shardedUniqueQueue{
		queues:   queues,
		inflight: make(map[Key]bool),
	}
}

// AddOrUpdate adds a request to be queued, it is thread safe.
// If the object in the request exists in the queue, then its
// object would be overridden
func (q *shardedUniqueQueue) AddIfAbsent(r Request) {
	added := q.addIfAbsent(r.Key)
	if added {
		q.queues[q.bucket(r.Key)] <- func() Request {
			q.deleteFromStore(r.Key)
			return r
		}
	}
}

func (q *shardedUniqueQueue) GetConsumers() []<-chan func() Request {
	res := make([]<-chan func() Request, 0, len(q.queues))
	for _, queue := range q.queues {
		res = append(res, queue)
	}
	return res
}

func (q *shardedUniqueQueue) bucket(key Key) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key.Name))
	return h.Sum32() % uint32(len(q.queues))
}

func (q *shardedUniqueQueue) addIfAbsent(key Key) bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	if _, ok := q.inflight[key]; ok {
		return false
	}
	q.inflight[key] = true
	return true
}

func (q *shardedUniqueQueue) deleteFromStore(key Key) {
	q.lock.Lock()
	defer q.lock.Unlock()
	delete(q.inflight, key)
}
