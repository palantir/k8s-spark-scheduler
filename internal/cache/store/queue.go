package store

import (
	"hash/fnv"
	"sync"
)

// ShardedUniqueQueue is a queue of write requests
// for objects. It compacts consecutive create and update
// requests, provides a slice of channels for consumers.
// Requests consumed from these channels are mutually
// exclusive
type ShardedUniqueQueue interface {
	AddIfAbsent(Request)
	GetConsumers() []<-chan func() Request
}

// NewShardedUniqueQueue creates a sharded queue of write requests
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
