package wal

import (
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func mustOpen(t *testing.T, path string) *WAL {
	t.Helper()
	w, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return w
}

func TestAppendReplayRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n1.wal")
	w := mustOpen(t, path)
	want := []Entry{
		{Term: 1, Index: 1, Op: "put", Key: "a", Value: []byte("1"), TS: 100},
		{Term: 1, Index: 2, Op: "put", Key: "b", Value: []byte("2"), TS: 200},
		{Term: 2, Index: 3, Op: "del", Key: "a", TS: 300},
	}
	for _, e := range want {
		if err := w.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if idx, term := w.Last(); idx != 3 || term != 2 {
		t.Fatalf("last = (%d, %d); want (3, 2)", idx, term)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := mustOpen(t, path)
	defer reopened.Close()
	got := reopened.After(0, 0)
	if len(got) != len(want) {
		t.Fatalf("replayed %d entries; want %d", len(got), len(want))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replayed = %+v; want %+v", got, want)
	}
}

func TestTornTailRecovered(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n1.wal")
	w := mustOpen(t, path)
	for i := uint64(1); i <= 2; i++ {
		if err := w.Append(Entry{Term: 1, Index: i, Op: "put", Key: "k", Value: []byte("v"), TS: 100}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Simulate a crash mid-append: a complete header plus a partial payload.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 64, 1, 2, 3, 4, 5, 6}); err != nil { // claims 64-byte payload, writes 2
		t.Fatal(err)
	}
	f.Close()

	reopened := mustOpen(t, path)
	if got := reopened.Len(); got != 2 {
		t.Fatalf("entries after repair = %d; want 2", got)
	}
	// The repaired log must stay usable.
	if err := reopened.Append(Entry{Term: 1, Index: 3, Op: "put", Key: "k", Value: []byte("v2"), TS: 200}); err != nil {
		t.Fatalf("append after repair: %v", err)
	}
	reopened.Close()

	third := mustOpen(t, path)
	defer third.Close()
	if got := third.Len(); got != 3 {
		t.Fatalf("entries after reopen = %d; want 3", got)
	}
}

func TestCorruptChecksumRecovered(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n1.wal")
	w := mustOpen(t, path)
	if err := w.Append(Entry{Term: 1, Index: 1, Op: "put", Key: "a", Value: []byte("1"), TS: 100}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Append(Entry{Term: 1, Index: 2, Op: "put", Key: "b", Value: []byte("2"), TS: 200}); err != nil {
		t.Fatalf("append: %v", err)
	}
	w.Close()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-3] ^= 0xFF // flip bits in the last record's payload
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	reopened := mustOpen(t, path)
	defer reopened.Close()
	if got := reopened.Len(); got != 1 {
		t.Fatalf("entries after corruption = %d; want 1 (good prefix)", got)
	}
}

func TestConcurrentAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n1.wal")
	w := mustOpen(t, path)

	const goroutines, perG = 8, 25
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				e := Entry{Term: 1, Index: uint64(g*perG + i + 1), Op: "put", Key: "k", Value: []byte("v"), TS: 1}
				if err := w.Append(e); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if got, want := w.Len(), goroutines*perG; got != want {
		t.Fatalf("len = %d; want %d", got, want)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := mustOpen(t, path)
	defer reopened.Close()
	if got := reopened.Len(); got != goroutines*perG {
		t.Fatalf("replayed = %d; want %d", got, goroutines*perG)
	}
}

func TestAfterWithLimit(t *testing.T) {
	w := mustOpen(t, filepath.Join(t.TempDir(), "n1.wal"))
	defer w.Close()
	for i := uint64(1); i <= 5; i++ {
		if err := w.Append(Entry{Term: 1, Index: i, Op: "put", Key: "k", Value: []byte("v"), TS: 1}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	got := w.After(2, 2)
	if len(got) != 2 || got[0].Index != 3 || got[1].Index != 4 {
		t.Fatalf("after(2, 2) = %+v; want indexes 3, 4", got)
	}
	if got := w.After(5, 0); len(got) != 0 {
		t.Fatalf("after(5, 0) = %+v; want empty", got)
	}
}
