package keyvaluestore

import (
	"sync"
	"time"
)

type KeyValue struct {
	Value  string
	Expiry time.Time
}

type Shard struct {
	data      map[string]KeyValue
	mu        map[string]*sync.RWMutex
	muControl sync.RWMutex
}

type KeyValueStore struct {
	shards   []*Shard
	replicas int
}

func NewKeyValueStore(numShards, numReplicas int) *KeyValueStore {
	store := &KeyValueStore{
		shards:   make([]*Shard, numShards),
		replicas: numReplicas,
	}

	for i := 0; i < numShards; i++ {
		store.shards[i] = &Shard{data: make(map[string]KeyValue)}
	}

	return store
}

func (kv *KeyValueStore) Set(key, value string, ttl time.Duration) {
	shardIndex := kv.GetShardIndex(key)
	shard := kv.shards[shardIndex]
	shard.getLock(key).Lock()
	defer shard.getLock(key).Unlock()
	expiry := time.Now().Add(ttl)

	shard.data[key] = KeyValue{
		Value:  value,
		Expiry: expiry,
	}
}

func (kv *KeyValueStore) Get(key string) (string, bool) {
	shardIndex := kv.GetShardIndex(key)
	shard := kv.shards[shardIndex]

	shard.getLock(key).RLock()
	defer shard.getLock(key).RUnlock()

	item, ok := shard.data[key]
	if !ok {
		return "", false
	}

	if item.Expiry.IsZero() || time.Now().Before(item.Expiry) {
		return item.Value, true
	}
	shard.deleteKey(key)
	return "", false
}

func (kv *Shard) getLock(key string) *sync.RWMutex {
	kv.muControl.Lock()
	defer kv.muControl.Unlock()

	if kv.mu == nil {
		kv.mu = make(map[string]*sync.RWMutex)
	}

	_, ok := kv.mu[key]
	if !ok {
		kv.mu[key] = &sync.RWMutex{}
	}
	return kv.mu[key]
}

func (sh *Shard) deleteKey(key string) {
	sh.muControl.Lock()
	defer sh.muControl.Unlock()

	delete(sh.data, key)
	delete(sh.mu, key)
}

func fnvHash(data string) uint32 {
	const prime = 16777619
	hash := uint32(2166136261)

	for i := 0; i < len(data); i++ {
		hash ^= uint32(data[i])
		hash *= prime
	}

	return hash
}

func (kv *KeyValueStore) GetShardIndex(key string) int {
	hash := fnvHash(key)
	return int(hash) % len(kv.shards)
}
