package store

import (
	"context"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strconv"
	"sync"
)

type ObjectStore interface {
	Put(metav1.Object)
	PutIfNewer(context.Context, metav1.Object) bool
	PutIfAbsent(metav1.Object) bool
	Get(Key) (metav1.Object, bool)
	Delete(Key)
	List() []metav1.Object
}

type objectStore struct {
	store map[Key]metav1.Object
	lock  sync.RWMutex
}

func NewStore() *objectStore {
	return &objectStore{
		store: make(map[Key]metav1.Object),
	}
}

func (s *objectStore) Put(obj metav1.Object) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.store[KeyOf(obj)] = obj
}

func (s *objectStore) PutIfNewer(ctx context.Context, obj metav1.Object) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	key := KeyOf(obj)
	currentObj, ok := s.store[key]
	if ok && resourceVersion(ctx, currentObj) >= resourceVersion(ctx, obj) {
		return false
	}
	s.store[key] = obj
	return true
}

func (s *objectStore) PutIfAbsent(obj metav1.Object) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	k := KeyOf(obj)
	_, ok := s.store[k]
	if ok {
		return false
	}
	s.store[k] = obj
	return true
}

func (s *objectStore) Get(key Key) (metav1.Object, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	obj, exists := s.store[key]
	return obj, exists
}

func (s *objectStore) Delete(key Key) {
	_, ok := s.Get(key)
	if !ok {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.store, key)
}

func (s *objectStore) List() []metav1.Object {
	s.lock.RLock()
	defer s.lock.RUnlock()
	res := make([]metav1.Object, 0, len(s.store))
	for _, o := range s.store {
		res = append(res, o)
	}
	return res
}

func resourceVersion(ctx context.Context, obj metav1.Object) uint64 {
	rv := obj.GetResourceVersion()
	if len(rv) == 0 {
		return 0
	}
	version, err := strconv.ParseUint(rv, 10, 64)
	if err != nil {
		svc1log.FromContext(ctx).Error("failed to parse resourceVersion, using 0",
			svc1log.SafeParam("objectNamespace", obj.GetNamespace()),
			svc1log.SafeParam("objectName", obj.GetNamespace()),
			svc1log.Stacktrace(err))
		return 0
	}
	return version
}
