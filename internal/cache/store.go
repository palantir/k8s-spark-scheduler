package cache

import (
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sync"
)

type objectStore struct {
	store map[string]metav1.Object
	lock  sync.RWMutex
}

func NewStore() *objectStore {
	return &objectStore{
		store: make(map[string]metav1.Object),
	}
}

func Key(obj metav1.Object) string {
	return fmt.Sprintf("%v/%v", obj.GetNamespace(), obj.GetName())
}

func (s *objectStore) Put(obj metav1.Object) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.store[Key(obj)] = obj
}

func (s *objectStore) PutIfAbsent(obj metav1.Object) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.store[Key(obj)]; !ok {
		s.store[Key(obj)] = obj
	}
}

func (s *objectStore) Get(key string) metav1.Object {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.store[key]
}

func (s *objectStore) Delete(key string) metav1.Object {
	obj := s.Get(key)
	if obj == nil {
		return nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.store, key)
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
