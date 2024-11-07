package keyvaluestore

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestKeyValueStore(t *testing.T) {
	kv := NewKeyValueStore(2,1)
	kv.Set("k1","v1",10.*time.Second)
	kv.Set("k2","v2",10.*time.Second)

	testGet(t,kv,"k1","v1")
	testGet(t,kv,"k2","v2")

	testGetNonExisting(t,kv,"k3")

	testExpiry(t,kv,"k1",10*time.Second)
	testConcurrentAccess(t,kv)
}

func testGet(t *testing.T, kv *KeyValueStore, key, expectedVal string) {
	t.Helper()

	val,ok := kv.Get(key)
	if !ok || val != expectedVal {
		t.Errorf("expected %s for key %s, but got %s",expectedVal,key,val)
	}
}

func testGetNonExisting(t *testing.T, kv *KeyValueStore, key string) {
	t.Helper()
	val, ok := kv.Get(key)
	if ok || val != "" {
		t.Errorf("expected non existence for key %s, but got %s", key, val)
	}
}

func testExpiry(t *testing.T, kv *KeyValueStore, key string, ttl time.Duration) {
	t.Helper()
	time.Sleep(ttl + 2*time.Second)

	val,ok := kv.Get(key)
	if val!= "" || ok {
		t.Errorf("expected key %s to be expired but got %s",key,val)
	}
}

func testConcurrentAccess(t *testing.T, kv *KeyValueStore) {
	t.Helper()

	kv.Set("key1","val1",time.Second*30)
	var wg sync.WaitGroup

	numReaders := 5
	numWriters := 5

	for i:=0; i< numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val, ok := kv.Get("key1")
			if !ok || val != "val1" {
				t.Errorf("concurrent read failed, expected val1 got %s",val)
			}
		}()
	}
	for i:=0;i<numWriters;i++ {
		wg.Add(1)
		go func (i int) {
			defer wg.Done()
			newVal := "val_" + strconv.Itoa(i)
			kv.Set("key2",newVal,10*time.Second)
		}(i)
	}
	wg.Wait()

	time.Sleep(time.Second*2)
	_, ok := kv.Get("key2")
	//_ := "val_" + strconv.Itoa(numWriters-1)
	if !ok {
		t.Errorf("Concurrent writes did not work properly")
	}
}