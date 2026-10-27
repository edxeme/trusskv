// Package node implements truss's Raft-lite consensus state machine:
// leader election by majority vote plus monotonic log replication.
//
// It is a teaching-grade implementation, deliberately small: every
// simplification is listed in docs/design.md. The three rules that hold
// without exception are:
//
//   - one leader per term, chosen by a majority of the cluster;
//   - log indexes never repeat within a term;
//   - nodes with a longer (term, index) tail win elections.
package node

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edxeme/trusskv/internal/store"
	"github.com/edxeme/trusskv/internal/wal"
)

// Role is a node's role in the consensus protocol.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// Transport carries consensus RPCs to a remote peer. It is implemented by
// internal/httptransport and faked in tests.
type Transport interface {
	RequestVote(ctx context.Context, peer string, req VoteRequest) (VoteResponse, error)
	AppendEntries(ctx context.Context, peer string, req AppendRequest) (AppendResponse, error)
}

// VoteRequest asks a peer for its vote in an election.
type VoteRequest struct {
	Term        uint64 `json:"term"`
	LastIndex   uint64 `json:"last_index"`
	LastTerm    uint64 `json:"last_term"`
	CandidateID string `json:"candidate_id"`
}

// VoteResponse carries a peer's vote decision.
type VoteResponse struct {
	Term    uint64 `json:"term"`
	Granted bool   `json:"granted"`
}

// AppendEntries replicates log entries; an empty Entries slice is a heartbeat.
type AppendRequest struct {
	Term     uint64      `json:"term"`
	LeaderID string      `json:"leader_id"`
	Entries  []wal.Entry `json:"entries,omitempty"`
}

// AppendResponse reports the follower's log position after the call.
type AppendResponse struct {
	Term  uint64 `json:"term"`
	Index uint64 `json:"index"`
	OK    bool   `json:"ok"`
}

// Config configures a node. Zero-value durations get production defaults.
type Config struct {
	ID        string   // unique within the cluster
	Advertise string   // URL peers use to reach this node; excluded from fan-out
	Peers     []string // peer URLs; may redundantly include self
	Transport Transport
	Store     *store.Store
	Log       *wal.WAL

	Heartbeat          time.Duration // leader heartbeat interval
	ElectionTimeoutMin time.Duration // randomized election timeout lower bound
	ElectionTimeoutMax time.Duration // randomized election timeout upper bound
	RPCTimeout         time.Duration // deadline for one consensus RPC

	// now is injectable for tests.
	now func() time.Time
}

// Node is a single consensus participant.
type Node struct {
	cfg     Config
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started time.Time

	mu       sync.RWMutex
	term     uint64
	role     Role
	votedFor string
	leader   string
	beat     time.Time // last heartbeat received or vote granted

	// matchIndex is the highest log index each peer is known to hold.
	// Leaders use it to decide what to resend and when an entry has quorum.
	matchMu    sync.Mutex
	matchIndex map[string]uint64
}

// New validates the config and returns a stopped node. Call Start to run it.
func New(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("truss: node id is required")
	}
	if cfg.Transport == nil {
		return nil, errors.New("truss: transport is required")
	}
	if cfg.Store == nil || cfg.Log == nil {
		return nil, errors.New("truss: store and log are required")
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = 100 * time.Millisecond
	}
	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = 250 * time.Millisecond
	}
	if cfg.ElectionTimeoutMax < cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = 2 * cfg.ElectionTimeoutMin
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = time.Second
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Node{
		cfg:        cfg,
		ctx:        ctx,
		cancel:     cancel,
		started:    cfg.now(),
		beat:       cfg.now(),
		matchIndex: make(map[string]uint64),
	}, nil
}

// Start launches the election/heartbeat loop.
func (n *Node) Start() {
	n.wg.Add(1)
	go n.loop()
}

// Stop cancels the loop and waits for tracked goroutines to finish.
func (n *Node) Stop() {
	n.cancel()
	n.wg.Wait()
}

// Status is a point-in-time view of the node, served by the API.
type Status struct {
	ID        string   `json:"id"`
	Role      string   `json:"role"`
	Term      uint64   `json:"term"`
	Leader    string   `json:"leader"`
	LastIndex uint64   `json:"last_index"`
	Keys      int      `json:"keys"`
	Peers     []string `json:"peers"`
	UptimeMS  int64    `json:"uptime_ms"`
}

// Status returns the node's current status.
func (n *Node) Status() Status {
	n.mu.RLock()
	defer n.mu.RUnlock()
	lastIndex, _ := n.cfg.Log.Last()
	return Status{
		ID:        n.cfg.ID,
		Role:      n.role.String(),
		Term:      n.term,
		Leader:    n.leader,
		LastIndex: lastIndex,
		Keys:      n.cfg.Store.Len(),
		Peers:     append([]string(nil), n.cfg.Peers...),
		UptimeMS:  n.cfg.now().Sub(n.started).Milliseconds(),
	}
}

// IsLeader reports whether this node currently leads and names the leader
// it knows, if any.
func (n *Node) IsLeader() (bool, string) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.role == Leader, n.leader
}

// loop is the heartbeat/election driver. Leaders heartbeat on a fixed
// schedule; followers and candidates campaign when the election timeout
// elapses without hearing from a leader.
func (n *Node) loop() {
	defer n.wg.Done()
	timer := time.NewTimer(n.electionTimeout())
	defer timer.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-timer.C:
			if n.currentRole() == Leader {
				n.broadcast()
				timer.Reset(n.cfg.Heartbeat)
				continue
			}
			timeout := n.electionTimeout()
			if time.Since(n.lastBeat()) >= timeout {
				n.startElection()
			}
			timer.Reset(timeout)
		}
	}
}

// startElection bumps the term, votes for itself, and fans out RequestVote
// RPCs. Winning a majority promotes the node to leader in that term.
func (n *Node) startElection() {
	n.mu.Lock()
	n.role = Candidate
	n.term++
	term := n.term
	n.votedFor = n.cfg.ID
	n.leader = ""
	lastIndex, lastTerm := n.cfg.Log.Last()
	n.mu.Unlock()

	var granted atomic.Int64
	granted.Store(1) // self-vote
	majority := int64(n.majority())

	var wg sync.WaitGroup
	for _, peer := range n.remotePeers() {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
			defer cancel()
			resp, err := n.cfg.Transport.RequestVote(ctx, peer, VoteRequest{
				Term:        term,
				LastIndex:   lastIndex,
				LastTerm:    lastTerm,
				CandidateID: n.cfg.ID,
			})
			if err != nil {
				return
			}
			if resp.Term > n.currentTerm() {
				n.stepDown(resp.Term)
				return
			}
			if resp.Granted && resp.Term == term {
				granted.Add(1)
			}
		}(peer)
	}

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		wg.Wait()
		select {
		case <-n.ctx.Done():
			return
		default:
		}
		if granted.Load() >= majority {
			n.becomeLeader(term)
		}
	}()
}

// becomeLeader promotes the node if it is still the term's candidate, then
// immediately heartbeats to establish authority before the next timeout.
func (n *Node) becomeLeader(term uint64) {
	n.mu.Lock()
	if n.role != Candidate || n.term != term {
		n.mu.Unlock()
		return
	}
	n.role = Leader
	n.leader = n.cfg.ID
	n.mu.Unlock()

	n.broadcast()
}

// HandleVote processes a RequestVote RPC from a peer.
func (n *Node) HandleVote(_ context.Context, req VoteRequest) VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := VoteResponse{Term: n.term}
	if req.Term < n.term {
		return resp
	}
	if req.Term > n.term {
		n.term = req.Term
		n.role = Follower
		n.votedFor = ""
		n.leader = ""
		resp.Term = req.Term
	}
	lastIndex, lastTerm := n.cfg.Log.Last()
	upToDate := req.LastTerm > lastTerm ||
		(req.LastTerm == lastTerm && req.LastIndex >= lastIndex)
	if (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate {
		n.votedFor = req.CandidateID
		n.beat = n.cfg.now() // a granted vote defers our own campaign
		resp.Granted = true
	}
	return resp
}

// HandleAppend processes an AppendEntries RPC: heartbeats refresh the
// leader lease; entries are appended to the local log and applied to the
// store in order. Duplicate deliveries (index already present) are skipped.
func (n *Node) HandleAppend(_ context.Context, req AppendRequest) AppendResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := AppendResponse{Term: n.term}
	if req.Term < n.term {
		return resp
	}
	if req.Term > n.term {
		n.term = req.Term
		n.votedFor = ""
	}
	resp.Term = n.term
	n.role = Follower
	n.leader = req.LeaderID
	n.beat = n.cfg.now()

	lastIndex, _ := n.cfg.Log.Last()
	for _, e := range req.Entries {
		if e.Index <= lastIndex {
			continue // duplicate delivery; already applied
		}
		if err := n.cfg.Log.Append(e); err != nil {
			return resp // OK stays false
		}
		n.applyEntry(e)
		lastIndex = e.Index
	}
	resp.Index = lastIndex
	resp.OK = true
	return resp
}

// stepDown adopts a strictly higher term and reverts to follower.
func (n *Node) stepDown(term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if term <= n.term {
		return
	}
	n.term = term
	n.role = Follower
	n.votedFor = ""
	n.leader = ""
}

// broadcast heartbeats every peer asynchronously.
func (n *Node) broadcast() {
	for _, peer := range n.remotePeers() {
		n.goRPC(func() { n.sendHeartbeat(peer) })
	}
}

// sendHeartbeat keeps a peer current: every call carries the entries the
// peer is missing (catch-up), or nothing when it is already up to date.
// A reply carrying a higher term forces a step-down.
func (n *Node) sendHeartbeat(peer string) {
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	defer cancel()
	resp, err := n.cfg.Transport.AppendEntries(ctx, peer, n.appendRequest(peer))
	if err != nil {
		return
	}
	if resp.Term > n.currentTerm() {
		n.stepDown(resp.Term)
		return
	}
	if resp.OK {
		n.setMatch(peer, resp.Index)
	}
}

func (n *Node) applyEntry(e wal.Entry) {
	n.cfg.Store.Apply(e.Op, e.Key, e.Value, e.Term, e.Index, e.TS)
}

// remotePeers returns configured peers minus this node's own address.
func (n *Node) remotePeers() []string {
	peers := make([]string, 0, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		if p != "" && p != n.cfg.Advertise {
			peers = append(peers, p)
		}
	}
	return peers
}

// majority is the smallest vote count that wins an election, counting self.
func (n *Node) majority() int {
	remote := len(n.remotePeers())
	return (remote+1)/2 + 1
}

func (n *Node) electionTimeout() time.Duration {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	return n.cfg.ElectionTimeoutMin + time.Duration(rand.Int64N(int64(span)+1))
}

// goRPC runs a best-effort RPC in a goroutine tracked by the node's WaitGroup.
func (n *Node) goRPC(fn func()) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		fn()
	}()
}

func (n *Node) currentRole() Role {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.role
}

func (n *Node) currentTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.term
}

func (n *Node) lastBeat() time.Time {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.beat
}
