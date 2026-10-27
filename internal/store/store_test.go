package store

import (
	"sort"
	"sync"
	"testing"
)

func TestApplyAndGetValue(t *testing.T) {
	s := New()
	if !s.Apply(OpPut, "a", []byte("1"), 1, 1, 100) {
		t.Fatal("first put should apply")
	}
	rec, ok := s.Get("a")
	if !ok || string(rec.Value) != "1" {
		t.Fatalf("get = %v, %v; want value 1", rec, ok)
	}
}

func TestApplyIdempotentForStaleEntries(t *testing.T) {
	s := New()
	s.Apply(OpPut, "a", []byte("1"), 1, 1, 100)
	if s.Apply(OpPut, "a", []byte("0"), 1, 1, 50) {
		t.Fatal("older timestamp must not apply")
	}
	if s.Apply(OpPut, "a", []byte("2"), 1, 1, 100) {
		t.Fatal("same timestamp with lower index must not apply")
	}
	rec, _ := s.Get("a")
	if string(rec.Value) != "1" {
		t.Fatalf("value = %q; want %q", rec.Value, "1")
	}
}

func TestTieBrokenByTermThenIndex(t *testing.T) {
	s := New()
	s.Apply(OpPut, "a", []byte("old"), 1, 5, 100)
	if !s.Apply(OpPut, "a", []byte("newterm"), 2, 1, 100) {
		t.Fatal("higher term must win a timestamp tie")
	}
	if !s.Apply(OpPut, "a", []byte("newindex"), 2, 6, 100) {
		t.Fatal("higher index must win a term tie")
	}
	rec, _ := s.Get("a")
	if string(rec.Value) != "newindex" {
		t.Fatalf("value = %q; want newindex", rec.Value)
	}
}

func TestDeleteKeepsTombstone(t *testing.T) {
	s := New()
	s.Apply(OpPut, "a", []byte("1"), 1, 1, 100)
	if !s.Apply(OpDelete, "a", nil, 1, 2, 200) {
		t.Fatal("delete should apply")
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("deleted key must not be readable")
	}
	// A stale put after the tombstone must not resurrect the key.
	if s.Apply(OpPut, "a", []byte("zombie"), 1, 1, 150) {
		t.Fatal("stale put must not apply over tombstone")
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("zombie write resurrected the key")
	}
	// The tombstone is still tracked so ordering holds.
	if len(s.Snapshot()) != 1 {
		t.Fatal("tombstone should remain in the snapshot")
	}
}

func TestKeysSortedAndSkipTombstones(t *testing.T) {
	s := New()
	s.Apply(OpPut, "b", []byte("1"), 1, 1, 100)
	s.Apply(OpPut, "a", []byte("1"), 1, 1, 100)
	s.Apply(OpPut, "c", []byte("1"), 1, 1, 100)
	s.Apply(OpDelete, "b", nil, 1, 2, 200)
	got := s.Keys()
	want := []string{"a", "c"}
	if len(got) != len(want) {
		t.Fatalf("keys = %v; want %v", got, want)
	}
	sort.Strings(got)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("keys = %v; want %v", got, want)
		}
	}
	if s.Len() != 2 {
		t.Fatalf("len = %d; want 2", s.Len())
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				s.Apply(OpPut, "k", []byte("v"), 1, uint64(i*50+j+1), int64(1000+j))
				s.Get("k")
				s.Keys()
				s.Snapshot()
			}
		}(i)
	}
	wg.Wait()
}
