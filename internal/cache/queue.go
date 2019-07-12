package cache

import (
	"hash/fnv"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sync"
)

const (
	createRequest string    = "create"
	updateRequest string    = "update"
	deleteRequest string    = "delete"
	tombstone     types.UID = types.UID("tombstone")
)

type request struct {
	metav1.Object
	_type string
}

type modifiableQueue struct {
	Buckets int
	queues  []chan types.UID
	store   map[types.UID]*request
	lock    sync.RWMutex
}

// NewModifiableQueue creates a bucketed queue with modifiable
// items.
func NewModifiableQueue(buckets int) *modifiableQueue {
	queues := make([]chan types.UID, 0, buckets)
	for i := 0; i < buckets; i++ {
		queues = append(queues, make(chan types.UID, 100))
	}
	return &modifiableQueue{
		Buckets: buckets,
		queues:  queues,
		store:   make(map[types.UID]*request),
	}
}

// AddOrUpdate adds a request to be queued, it is thread safe
func (q *modifiableQueue) AddOrUpdate(r *request) {
	added := q.addOrUpdateStore(r)
	if added {
		q.queues[q.bucket(r.GetUID())] <- r.GetUID()
	}
}

// Get returns the next request in nth bucket
// calling Get with the same n is not thread safe
func (q *modifiableQueue) Get(n int) *request {
	uid := <-q.queues[n]
	return q.getAndDeleteFromStore(uid)
}

func (q *modifiableQueue) bucket(uid types.UID) uint32 {
	h := fnv.New32a()
	h.Write([]byte(uid))
	return h.Sum32() % uint32(len(q.queues))
}

func (q *modifiableQueue) addOrUpdateStore(newRequest *request) bool {
	q.lock.Lock()
	defer q.lock.Unlock()

	added := false
	r := q.store[newRequest.GetUID()]

	if r == nil {
		added = true
		r = newRequest
	} else {
		r.Object = newRequest.Object
	}

	q.store[r.GetUID()] = r
	return added
}

func (q *modifiableQueue) getAndDeleteFromStore(uid types.UID) *request {
	q.lock.Lock()
	defer q.lock.Unlock()

	r := q.store[uid]
	delete(q.store, uid)

	return r
}
