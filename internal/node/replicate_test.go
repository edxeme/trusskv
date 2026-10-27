package node

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/edxeme/trusskv/internal/store"
	"github.com/edxeme/trusskv/internal/wal"
)

func TestPutFailsWhenNotLeader(t *testing.T) {
	n := newTestNode(t, "n1", []string{"http://n2"}, &fakeTransport{})
	if err := n.Put("k", []byte("v")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("put on follower = %v; want ErrNotLeader", err)
	}
}

func TestPutWithQuorum(t *testing.T) {
	tr := &fakeTransport{}
	n := newTestNode(t, "n1", []string{"http://n2", "http://n3"}, tr)
	forceLeader(n, 7)

	if err := n.Put("color", []byte("teal")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if rec, ok := n.Get("color"); !ok || string(rec.Value) != "teal" {
		t.Fatalf("leader store = %+v; want color=teal", rec)
	}

	// The replicated entries must carry the leader's term.
	appends := tr.recordedAppends()
	if len(appends) == 0 {
		t.Fatal("no AppendEntries fanned out")
	}
	for _, req := range appends {
		if req.Term != 7 || req.LeaderID != "n1" {
			t.Fatalf("append request = %+v; want term 7 from n1", req)
		}
		if len(req.Entries) == 0 || req.Entries[0].Key != "color" {
			t.Fatalf("append request = %+v; want the color entry", req)
		}
	}
	if _, term := n.cfg.Log.Last(); term != 7 {
		t.Fatalf("log term = %d; want 7", term)
	}
}

func TestPutWithoutQuorum(t *testing.T) {
	tr := &fakeTransport{
		appendFn: func(req AppendRequest) (AppendResponse, error) {
			return AppendResponse{}, errors.New("peer unreachable")
		},
	}
	n := newTestNode(t, "n1", []string{"http://n2", "http://n3"}, tr)
	forceLeader(n, 1)

	if err := n.Put("k", []byte("v")); !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("put = %v; want ErrNoQuorum", err)
	}
	// Leader-local durability: the entry is in the log and applied even
	// though the majority did not acknowledge it.
	if idx, _ := n.cfg.Log.Last(); idx != 1 {
		t.Fatalf("log index = %d; want 1", idx)
	}
	if _, ok := n.Get("k"); !ok {
		t.Fatal("leader-local write must be applied before replication")
	}
}

func TestPutRejectsLowerTermAcks(t *testing.T) {
	// A peer in an older term must not count toward quorum.
	tr := &fakeTransport{
		appendFn: func(req AppendRequest) (AppendResponse, error) {
			return AppendResponse{Term: req.Term - 1, Index: 1, OK: true}, nil
		},
	}
	n := newTestNode(t, "n1", []string{"http://n2"}, tr)
	forceLeader(n, 5)

	if err := n.Put("k", []byte("v")); !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("put = %v; want ErrNoQuorum (stale-term ack)", err)
	}
}

func TestDeleteWritesTombstone(t *testing.T) {
	tr := &fakeTransport{}
	n := newTestNode(t, "n1", nil, tr)
	forceLeader(n, 1)

	if err := n.Put("k", []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := n.Delete("k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := n.Get("k"); ok {
		t.Fatal("deleted key must not be readable")
	}
	if got := n.Status().Keys; got != 0 {
		t.Fatalf("keys = %d; want 0", got)
	}
}

func TestHeartbeatCatchesUpLaggingPeer(t *testing.T) {
	tr := &fakeTransport{}
	n := newTestNode(t, "n1", []string{"http://n2"}, tr)
	forceLeader(n, 1)

	// Three writes the peer has not seen yet.
	for _, kv := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		if err := n.Put(kv.k, []byte(kv.v)); err != nil {
			t.Fatalf("put %s: %v", kv.k, err)
		}
	}
	// Quorum writes already advanced the peer's match.
	if got := n.match("http://n2"); got != 3 {
		t.Fatalf("match = %d; want 3", got)
	}

	// Simulate a peer that lost everything: rewind and heartbeat.
	n.matchMu.Lock()
	n.matchIndex["http://n2"] = 0
	n.matchMu.Unlock()
	n.sendHeartbeat("http://n2")

	appends := tr.recordedAppends()
	last := appends[len(appends)-1]
	if len(last.Entries) != 3 || last.Entries[0].Index != 1 || last.Entries[2].Index != 3 {
		t.Fatalf("catch-up request = %+v; want entries 1..3", last)
	}
	if got := n.match("http://n2"); got != 3 {
		t.Fatalf("match after catch-up = %d; want 3", got)
	}

	// An up-to-date peer gets a pure heartbeat.
	n.sendHeartbeat("http://n2")
	last = tr.recordedAppends()[len(tr.recordedAppends())-1]
	if len(last.Entries) != 0 {
		t.Fatalf("heartbeat = %+v; want no entries", last)
	}
}

func TestConcurrentWritesGetSequentialIndexes(t *testing.T) {
	tr := &fakeTransport{}
	n := newTestNode(t, "n1", nil, tr)
	forceLeader(n, 2)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := n.Put(string(rune('a'+i)), []byte("v")); err != nil {
				t.Errorf("put %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if got := n.cfg.Log.Len(); got != 8 {
		t.Fatalf("log len = %d; want 8", got)
	}
	// Indexes must be unique and contiguous: they were assigned under the
	// state machine lock.
	seen := map[uint64]bool{}
	for _, e := range n.cfg.Log.After(0, 0) {
		if seen[e.Index] {
			t.Fatalf("duplicate log index %d", e.Index)
		}
		seen[e.Index] = true
	}
}

func TestApplyViaHandleAppendIsIdempotent(t *testing.T) {
	n := newTestNode(t, "n1", nil, &fakeTransport{})
	req := AppendRequest{
		Term:     1,
		LeaderID: "n2",
		Entries: []wal.Entry{
			{Term: 1, Index: 1, Op: store.OpPut, Key: "k", Value: []byte("v1"), TS: 100},
		},
	}
	n.HandleAppend(context.Background(), req)
	n.HandleAppend(context.Background(), req)
	if got := n.cfg.Log.Len(); got != 1 {
		t.Fatalf("log len = %d; want 1 after duplicate delivery", got)
	}
}
