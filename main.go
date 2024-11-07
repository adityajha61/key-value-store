package main

import (
	"encoding/json"
	"fmt"
	"key-value-store/keyvaluestore"
	"net/http"
	"time"
)

func HandleSet(kv *keyvaluestore.KeyValueStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Key string `json:"key"`
			Value string `json:"value"`
			TTL time.Duration `json:"ttl"`
		}

		err := json.NewDecoder(r.Body).Decode(&req)
		if(err!=nil) {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
		}
		
		if req.TTL == 0 {
			req.TTL = 5*time.Minute
		}

		fmt.Print(req)
		kv.Set(req.Key, req.Value, req.TTL)
		w.WriteHeader(http.StatusOK)
	}
}

func HandleGet(kv *keyvaluestore.KeyValueStore) http.HandlerFunc {
	return func(w http.ResponseWriter,r *http.Request) {
		key := r.URL.Query().Get("key")
		if(key == "") {
			http.Error(w,"Key param is missing", http.StatusBadRequest)
		}
		val, ok := kv.Get(key)
		if !ok {
			http.Error(w, "key not found", http.StatusNotFound)
		}
		resp := struct {
			Key string `json:"key"`
			Value string `json:"value"`
		}{Key: key, Value: val}
		w.Header().Set("Content-Type","application/json")
		json.NewEncoder(w).Encode(resp)
	}
} 
func main() {
	kv := keyvaluestore.NewKeyValueStore(4,2)
	http.HandleFunc("/set", HandleSet(kv))
	http.HandleFunc("/get", HandleGet(kv))

	port := 8080

	address := fmt.Sprintf(":%d", port)
	fmt.Printf("Starting server on port %s\n", address)
	err := http.ListenAndServe(address,nil)
	if(err!=nil){
		fmt.Printf("Error:%s\n",err)
	}
}