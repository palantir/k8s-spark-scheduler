package store

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sync"
)

type Key struct {
	Namespace string
	Name      string
}

type ObjectStore interface {
	Put(metav1.Object)
	PutIfAbsent(metav1.Object)
	Get(string, string) metav1.Object
	Delete(string, string) metav1.Object
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

func (s *objectStore) PutIfAbsent(obj metav1.Object) {
	s.lock.Lock()
	defer s.lock.Unlock()
	k := key(obj)
	if _, ok := s.store[k]; !ok {
		s.store[k] = obj
	}
}

func (s *objectStore) Get(namespace, name string) metav1.Object {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.store[Key{namespace, name}]
}

func (s *objectStore) Delete(namespace, name string) metav1.Object {
	obj := s.Get(namespace, name)
	if obj == nil {
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
