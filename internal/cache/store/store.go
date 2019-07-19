package store

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strconv"
	"sync"
)

type Key struct {
	Namespace string
	Name      string
}

type ObjectStore interface {
	Put(metav1.Object)
	PutIfNewer(metav1.Object) bool
	PutIfAbsent(metav1.Object) bool
	Get(string, string) (metav1.Object, bool)
	Delete(string, string) metav1.Object
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

func key(obj metav1.Object) Key {
	return Key{obj.GetNamespace(), obj.GetName()}
}

func (s *objectStore) Put(obj metav1.Object) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.store[key(obj)] = obj
}

func (s *objectStore) PutIfNewer(obj metav1.Object) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	key := key(obj)
	currentObj, ok := s.store[key]
	if ok && resourceVersion(currentObj) >= resourceVersion(obj) {
		return false
	}
	s.store[key] = obj
	return true
}

func (s *objectStore) PutIfAbsent(obj metav1.Object) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	k := key(obj)
	_, ok := s.store[k]
	if ok {
		return false
	}
	s.store[k] = obj
	return true
}

func (s *objectStore) Get(namespace, name string) (metav1.Object, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	obj, exists := s.store[Key{namespace, name}]
	return obj, exists
}

func (s *objectStore) Delete(namespace, name string) metav1.Object {
	obj, ok := s.Get(namespace, name)
	if !ok {
		return nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.store, Key{namespace, name})
	return obj
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

func resourceVersion(obj metav1.Object) uint64 {
	rv := obj.GetResourceVersion()
	if len(rv) == 0 {
		return 0
	}
	version, _ := strconv.ParseUint(rv, 10, 64) // TODO: error handling
	return version
}
