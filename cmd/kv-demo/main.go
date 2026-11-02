// kv-demo is a single-node, in-memory key-value HTTP server.
// It is the baseline the Raft-replicated store will later be compared against.
package main

import (
	"io"
	"log"
	"net/http"
	"sync"
)

// store is a map guarded by a RWMutex: many readers may hold the lock at once,
// but a writer needs exclusive access.
type store struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func newStore() *store {
	return &store{data: make(map[string][]byte)}
}

func (s *store) put(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *store) get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// newHandler wires the routes. It is separate from main so tests can use it
// with httptest without binding a real port.
func newHandler(s *store) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		// Cap the body so a client cannot make the server buffer unbounded data.
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			http.Error(w, "cannot read body", http.StatusBadRequest)
			return
		}
		s.put(r.PathValue("key"), body)
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		v, ok := s.get(r.PathValue("key"))
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write(v)
	})

	return mux
}

func main() {
	log.Println("kv-demo listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", newHandler(newStore())))
}
