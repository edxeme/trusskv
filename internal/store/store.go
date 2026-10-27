// Package store implements the versioned key-value state machine that
// every truss node applies to its replicated log.
//
// Ordering rule: a write wins over the current record when its timestamp
// is higher; ties are broken by log term, then log index. This keeps
// nodes deterministic when the same entries arrive in different orders.
package store

import (
	"sort"
	"sync"
	"time"
)

// Ops applied by the log.
const (
	OpPut    = "put"
	OpDelete = "del"
)

// Record is the value kept for a key plus the metadata used to order
// concurrent writes.
type Record struct {
	Value     []byte `json:"value"`
	Term      uint64 `json:"term"`
	Index     uint64 `json:"index"`
	Timestamp int64  `json:"timestamp"` // unix millis of the write
	Tombstone bool   `json:"tombstone,omitempty"`
}

// Newer reports whether write (ts, term, index) supersedes the current record.
func Newer(ts int64, term, index uint64, cur Record) bool {
	if ts != cur.Timestamp {
		return ts > cur.Timestamp
	}
	if term != cur.Term {
		return term > cur.Term
	}
	return index > cur.Index
}

// Store is an in-memory, goroutine-safe key-value state machine.
type Store struct {
	mu   sync.RWMutex
	data map[string]Record
}

// New returns an empty store.
func New() *Store {
	return &Store{data: make(map[string]Record)}
}

// NowMillis returns the current time in unix millis. It exists so callers
// stamp writes with a single, comparable clock.
func NowMillis() int64 { return time.Now().UnixMilli() }

// Apply applies a log entry to the state machine and reports whether it
// changed the stored record. Tombstones keep a deleted marker so versions
// survive across nodes.
func (s *Store) Apply(op, key string, value []byte, term, index uint64, ts int64) bool {
	rec := Record{Term: term, Index: index, Timestamp: ts}
	switch op {
	case OpPut:
		rec.Value = value
	case OpDelete:
		rec.Tombstone = true
	default:
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.data[key]
	if ok && !Newer(ts, term, index, cur) {
		return false
	}
	s.data[key] = rec
	return true
}

// Get returns the record for key and whether it exists. Tombstoned keys
// report exists=false.
func (s *Store) Get(key string) (Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.data[key]
	if !ok || rec.Tombstone {
		return Record{}, false
	}
	return rec, true
}

// Keys returns the live (non-tombstoned) keys, sorted.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k, rec := range s.data {
		if !rec.Tombstone {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// Len reports the number of live keys.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, rec := range s.data {
		if !rec.Tombstone {
			n++
		}
	}
	return n
}

// Snapshot copies the full state, tombstones included. Used by status pages.
func (s *Store) Snapshot() map[string]Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]Record, len(s.data))
	for k, rec := range s.data {
		out[k] = rec
	}
	return out
}
