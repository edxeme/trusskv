package node

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/edxeme/trusskv/internal/store"
	"github.com/edxeme/trusskv/internal/wal"
)

// fakeTransport records every RPC and scripts replies.
type fakeTransport struct {
	mu       sync.Mutex
	voteFn   func(req VoteRequest) (VoteResponse, error)
	appendFn func(req AppendRequest) (AppendResponse, error)

	votes   []VoteRequest
	appends []AppendRequest
}

func (f *fakeTransport) RequestVote(_ context.Context, _ string, req VoteRequest) (VoteResponse, error) {
	f.mu.Lock()
	f.votes = append(f.votes, req)
	fn := f.voteFn
	f.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	return VoteResponse{Term: req.Term, Granted: true}, nil
}

func (f *fakeTransport) AppendEntries(_ context.Context, _ string, req AppendRequest) (AppendResponse, error) {
	f.mu.Lock()
	f.appends = append(f.appends, req)
	fn := f.appendFn
	f.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	var last uint64
	for _, e := range req.Entries {
		last = e.Index
	}
	return AppendResponse{Term: req.Term, Index: last, OK: true}, nil
}

func (f *fakeTransport) recordedVotes() []VoteRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]VoteRequest(nil), f.votes...)
}

func (f *fakeTransport) recordedAppends() []AppendRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]AppendRequest(nil), f.appends...)
}

func newTestNode(t *testing.T, id string, peers []string, tr Transport) *Node {
	t.Helper()
	n, err := New(newTestConfig(t, id, peers, tr))
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	return n
}

func newTestConfig(t *testing.T, id string, peers []string, tr Transport) Config {
	t.Helper()
	st := store.New()
	log, err := wal.Open(filepath.Join(t.TempDir(), id+".wal"))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { log.Close() })
	return Config{
		ID:                 id,
		Peers:              peers,
		Transport:          tr,
		Store:              st,
		Log:                log,
		Heartbeat:          30 * time.Millisecond,
		ElectionTimeoutMin: 60 * time.Millisecond,
		ElectionTimeoutMax: 120 * time.Millisecond,
		RPCTimeout:         500 * time.Millisecond,
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// forceLeader marks the node as leader in the given term, bypassing an
// election. Only valid from the same package (white-box tests).
func forceLeader(n *Node, term uint64) {
	n.mu.Lock()
	n.term = term
	n.role = Leader
	n.leader = n.cfg.ID
	n.mu.Unlock()
}

func TestHandleVoteGrantsOncePerTerm(t *testing.T) {
	n := newTestNode(t, "n1", []string{"http://n2"}, &fakeTransport{})
	resp := n.HandleVote(context.Background(), VoteRequest{Term: 1, CandidateID: "n2"})
	if !resp.Granted || resp.Term != 1 {
		t.Fatalf("first vote = %+v; want granted in term 1", resp)
	}
	resp = n.HandleVote(context.Background(), VoteRequest{Term: 1, CandidateID: "n3"})
	if resp.Granted {
		t.Fatal("second candidate in the same term must be rejected")
	}
	resp = n.HandleVote(context.Background(), VoteRequest{Term: 2, CandidateID: "n3"})
	if !resp.Granted || resp.Term != 2 {
		t.Fatalf("new term vote = %+v; want granted in term 2", resp)
	}
}

func TestHandleVoteRejectsStaleCandidateLog(t *testing.T) {
	n := newTestNode(t, "n1", []string{"http://n2"}, &fakeTransport{})
	n.cfg.Log.Append(wal.Entry{Term: 2, Index: 5, Op: store.OpPut, Key: "k", TS: 1})
	resp := n.HandleVote(context.Background(), VoteRequest{Term: 3, LastIndex: 3, LastTerm: 1, CandidateID: "n2"})
	if resp.Granted {
		t.Fatal("candidate with a shorter log must be rejected")
	}
	resp = n.HandleVote(context.Background(), VoteRequest{Term: 3, LastIndex: 5, LastTerm: 2, CandidateID: "n2"})
	if !resp.Granted {
		t.Fatal("candidate with an equal-or-longer log must be granted")
	}
}

func TestHandleVoteRejectsLowerTerm(t *testing.T) {
	n := newTestNode(t, "n1", nil, &fakeTransport{})
	forceLeader(n, 4)
	resp := n.HandleVote(context.Background(), VoteRequest{Term: 3, CandidateID: "n2"})
	if resp.Granted || resp.Term != 4 {
		t.Fatalf("stale vote request = %+v; want rejected with term 4", resp)
	}
	if role := n.Status().Role; role != Leader.String() {
		t.Fatalf("leader must not step down on a stale term; role = %s", role)
	}
}

func TestHandleAppendAdoptsTermAndApplies(t *testing.T) {
	n := newTestNode(t, "n1", []string{"http://n2"}, &fakeTransport{})
	forceLeader(n, 3)

	resp := n.HandleAppend(context.Background(), AppendRequest{
		Term:     5,
		LeaderID: "n2",
		Entries: []wal.Entry{
			{Term: 5, Index: 1, Op: store.OpPut, Key: "a", Value: []byte("1"), TS: 100},
			{Term: 5, Index: 2, Op: store.OpPut, Key: "b", Value: []byte("2"), TS: 101},
		},
	})
	if !resp.OK || resp.Term != 5 || resp.Index != 2 {
		t.Fatalf("append response = %+v; want ok in term 5 at index 2", resp)
	}
	if v, ok := n.cfg.Store.Get("a"); !ok || string(v.Value) != "1" {
		t.Fatalf("store after append = %+v; want key a=1", v)
	}
	st := n.Status()
	if st.Role != Follower.String() || st.Leader != "n2" || st.Term != 5 {
		t.Fatalf("status after append = %+v; want follower of n2 in term 5", st)
	}
}

func TestHandleAppendSkipsDuplicateEntries(t *testing.T) {
	n := newTestNode(t, "n1", []string{"http://n2"}, &fakeTransport{})
	req := AppendRequest{
		Term:     1,
		LeaderID: "n2",
		Entries: []wal.Entry{
			{Term: 1, Index: 1, Op: store.OpPut, Key: "a", Value: []byte("1"), TS: 100},
			{Term: 1, Index: 2, Op: store.OpPut, Key: "b", Value: []byte("2"), TS: 101},
		},
	}
	if resp := n.HandleAppend(context.Background(), req); !resp.OK || resp.Index != 2 {
		t.Fatalf("first append = %+v; want ok at index 2", resp)
	}
	if resp := n.HandleAppend(context.Background(), req); !resp.OK || resp.Index != 2 {
		t.Fatalf("duplicate append = %+v; want ok at index 2", resp)
	}
	if got := n.cfg.Log.Len(); got != 2 {
		t.Fatalf("log len = %d; want 2 (no duplicates)", got)
	}
}

func TestStepDownOnHigherTermFromHeartbeat(t *testing.T) {
	n := newTestNode(t, "n1", []string{"http://n2"}, &fakeTransport{})
	forceLeader(n, 2)
	resp := n.HandleAppend(context.Background(), AppendRequest{Term: 3, LeaderID: "n2"})
	if !resp.OK || resp.Term != 3 {
		t.Fatalf("heartbeat response = %+v; want ok in term 3", resp)
	}
	if st := n.Status(); st.Role != Follower.String() || st.Term != 3 {
		t.Fatalf("status = %+v; want follower in term 3", st)
	}
}

func TestSingleNodeElectsItself(t *testing.T) {
	n := newTestNode(t, "n1", nil, &fakeTransport{})
	n.Start()
	defer n.Stop()
	waitFor(t, 5*time.Second, "single-node leadership", func() bool {
		ok, _ := n.IsLeader()
		return ok
	})
}

func TestElectionWinsMajorityAndHeartbeats(t *testing.T) {
	tr := &fakeTransport{}
	n := newTestNode(t, "n1", []string{"http://n2", "http://n3"}, tr)
	n.Start()
	defer n.Stop()

	waitFor(t, 5*time.Second, "leadership", func() bool {
		ok, _ := n.IsLeader()
		return ok
	})
	waitFor(t, 5*time.Second, "outgoing heartbeats", func() bool {
		return len(tr.recordedAppends()) > 0
	})
	if got := len(tr.recordedVotes()); got == 0 {
		t.Fatal("candidate sent no RequestVote RPCs")
	}
}

func TestElectionRetriesUntilMajority(t *testing.T) {
	// The only peer keeps refusing: the node must stay a candidate.
	tr := &fakeTransport{
		voteFn: func(req VoteRequest) (VoteResponse, error) {
			return VoteResponse{Term: req.Term, Granted: false}, nil
		},
	}
	n := newTestNode(t, "n1", []string{"http://n2"}, tr)
	n.Start()
	defer n.Stop()

	waitFor(t, 5*time.Second, "at least two election attempts", func() bool {
		votes := tr.recordedVotes()
		return len(votes) >= 2 && votes[1].Term > votes[0].Term
	})
	ok, _ := n.IsLeader()
	if ok {
		t.Fatal("node must not become leader without a majority")
	}
}
