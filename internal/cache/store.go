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

func key(obj metav1.Object) string {
	return keyFromNamespaceName(obj.GetNamespace(), obj.GetName())
}

func keyFromNamespaceName(namespace, name string) string {
	return fmt.Sprintf("%v/%v", namespace, name)
}

func (s *objectStore) Put(obj metav1.Object) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.store[key(obj)] = obj
}

func (s *objectStore) PutIfAbsent(obj metav1.Object) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.store[key(obj)]; !ok {
		s.store[key(obj)] = obj
	}
}

func (s *objectStore) Get(namespace, name string) metav1.Object {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.store[keyFromNamespaceName(namespace, name)]
}

func (s *objectStore) Delete(namespace, name string) metav1.Object {
	obj := s.Get(namespace, name)
	if obj == nil {
		return nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.store, keyFromNamespaceName(namespace, name))
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
