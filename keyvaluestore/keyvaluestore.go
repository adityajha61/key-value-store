package keyvaluestore

import (
	"sync"
	"time"
)

type KeyValue struct {
	Value string
	Expiry time.Time
}

type KeyValueStore struct {
	data map[string]KeyValue
	mu map[string]*sync.RWMutex
	muControl sync.RWMutex
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		data: make(map[string]KeyValue),
	}
}

func (kv *KeyValueStore) Set(key, value string, ttl time.Duration) {
	kv.getLock(key).Lock()
	defer kv.getLock(key).Unlock()
	expiry := time.Now().Add(ttl)
	kv.data[key] = KeyValue{
		Value: value,
		Expiry: expiry,
	}
}

func (kv *KeyValueStore) Get(key string) (string,bool) {
	kv.getLock(key).RLock()
	defer kv.getLock(key).RUnlock()

	item, ok := kv.data[key]
	if !ok {
		return "",false
	}

	if item.Expiry.IsZero() || time.Now().Before(item.Expiry) {
		return item.Value,true
	}
	kv.deleteKey(key)
	return "",false
}

func (kv *KeyValueStore) getLock(key string) *sync.RWMutex {
	kv.muControl.Lock()
	defer kv.muControl.Unlock()

	if kv.mu == nil {
		kv.mu = make(map[string]*sync.RWMutex)
	}

	_,ok := kv.mu[key]
	if !ok {
		kv.mu[key] = &sync.RWMutex{}
	}
	return kv.mu[key]
}

func (kv *KeyValueStore) deleteKey(key string){
	kv.muControl.Lock()
	defer kv.muControl.Unlock()

	delete(kv.data,key)
	delete(kv.mu,key)
}