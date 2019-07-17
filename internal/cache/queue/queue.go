package queue

import (
	"hash/fnv"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sync"
)

type ModifiableQueue interface {
	AddOrUpdate(WriteRequest)
	UpdateIfExists(metav1.Object) bool
	GetConsumers() []<-chan func() WriteRequest
}

type modifiableQueue struct {
	queues []chan func() WriteRequest
	store  map[types.UID]WriteRequest
	lock   sync.Mutex
}

// NewModifiableQueue creates a bucketed queue with modifiable
// items.
func NewModifiableQueue(buckets int) ModifiableQueue {
	queues := make([]chan func() WriteRequest, 0, buckets)
	for i := 0; i < buckets; i++ {
		queues = append(queues, make(chan func() WriteRequest, 100))
	}
	return &modifiableQueue{
		queues: queues,
		store:  make(map[types.UID]WriteRequest),
	}
}

// AddOrUpdate adds a request to be queued, it is thread safe.
// If the object in the request exists in the queue, then its
// object would be overridden
func (q *modifiableQueue) AddOrUpdate(r WriteRequest) {
	added := q.addOrUpdateStore(r)
	if added {
		q.queues[q.bucket(r.Object().GetUID())] <- func() WriteRequest {
			return q.getAndDeleteFromStore(r.Object().GetUID())
		}
	}
}

func (q *modifiableQueue) UpdateIfExists(obj metav1.Object) bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	r, ok := q.store[obj.GetUID()]
	if ok {
		r.SetObject(obj)
	}
	return ok
}

func (q *modifiableQueue) GetConsumers() []<-chan func() WriteRequest {
	res := make([]<-chan func() WriteRequest, 0, len(q.queues))
	for _, queue := range q.queues {
		res = append(res, queue)
	}
	return res
}

func (q *modifiableQueue) bucket(uid types.UID) uint32 {
	h := fnv.New32a()
	h.Write([]byte(uid))
	return h.Sum32() % uint32(len(q.queues))
}

func (q *modifiableQueue) addOrUpdateStore(newRequest WriteRequest) bool {
	q.lock.Lock()
	defer q.lock.Unlock()

	added := false
	r := q.store[newRequest.Object().GetUID()]

	if r == nil {
		added = true
		r = newRequest
	} else {
		r.SetObject(newRequest.Object())
	}

	q.store[r.Object().GetUID()] = r
	return added
}

func (q *modifiableQueue) getAndDeleteFromStore(uid types.UID) WriteRequest {
	q.lock.Lock()
	defer q.lock.Unlock()

	r := q.store[uid]
	delete(q.store, uid)

	return r
}
