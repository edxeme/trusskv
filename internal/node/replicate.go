package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/edxeme/trusskv/internal/store"
	"github.com/edxeme/trusskv/internal/wal"
)

// Errors returned by the write path.
var (
	ErrNotLeader = errors.New("truss: node is not the leader")
	ErrNoQuorum  = errors.New("truss: write not acknowledged by a majority")
)

// maxBatch bounds how many entries are piggybacked onto one AppendEntries.
const maxBatch = 128

// Put writes a value by routing it through the replicated log. The entry is
// durable on this node before replication starts; the returned error only
// reflects whether a majority acknowledged it. See docs/design.md for the
// exact guarantees.
func (n *Node) Put(key string, value []byte) error {
	return n.write(store.OpPut, key, value)
}

// Delete writes a tombstone for key through the same path as Put.
func (n *Node) Delete(key string) error {
	return n.write(store.OpDelete, key, nil)
}

// Get returns the local record for key. Followers may serve stale reads.
func (n *Node) Get(key string) (store.Record, bool) {
	return n.cfg.Store.Get(key)
}

// Snapshot copies the local state; followers may be stale.
func (n *Node) Snapshot() map[string]store.Record {
	return n.cfg.Store.Snapshot()
}

// Keys returns the live keys known to this node, sorted.
func (n *Node) Keys() []string {
	return n.cfg.Store.Keys()
}

func (n *Node) write(op, key string, value []byte) error {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	lastIndex, _ := n.cfg.Log.Last()
	entry := wal.Entry{
		Term:  n.term,
		Index: lastIndex + 1,
		Op:    op,
		Key:   key,
		Value: value,
		TS:    n.cfg.now().UnixMilli(),
	}
	if err := n.cfg.Log.Append(entry); err != nil {
		n.mu.Unlock()
		return fmt.Errorf("truss: append entry %d: %w", entry.Index, err)
	}
	n.applyEntry(entry)
	n.mu.Unlock()

	if n.replicateQuorum(n.term, entry.Index) < n.majority()-1 {
		return ErrNoQuorum
	}
	return nil
}

// replicateQuorum brings every peer up to date in parallel and returns how
// many remote peers acknowledged holding at least the target index in the
// leader's term.
func (n *Node) replicateQuorum(term, index uint64) int {
	var acks atomic.Int64
	var wg sync.WaitGroup
	for _, peer := range n.remotePeers() {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			if n.replicateTo(peer, term, index) {
				acks.Add(1)
			}
		}(peer)
	}
	wg.Wait()
	return int(acks.Load())
}

// replicateTo sends a peer everything after its last known match and reports
// whether the reply covers the target index in the given term.
func (n *Node) replicateTo(peer string, term, target uint64) bool {
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	defer cancel()
	resp, err := n.cfg.Transport.AppendEntries(ctx, peer, n.appendRequest(peer))
	if err != nil {
		return false
	}
	if resp.Term > n.currentTerm() {
		n.stepDown(resp.Term)
		return false
	}
	if !resp.OK || resp.Term != term {
		return false
	}
	n.setMatch(peer, resp.Index)
	return resp.Index >= target
}

// appendRequest builds the next AppendEntries for a peer: any entries the
// peer is missing (bounded by maxBatch), or a pure heartbeat when current.
func (n *Node) appendRequest(peer string) AppendRequest {
	req := AppendRequest{Term: n.currentTerm(), LeaderID: n.cfg.ID}
	req.Entries = n.cfg.Log.After(n.match(peer), maxBatch)
	return req
}

// match reports the highest log index the peer is known to hold.
func (n *Node) match(peer string) uint64 {
	n.matchMu.Lock()
	defer n.matchMu.Unlock()
	return n.matchIndex[peer]
}

// setMatch records that the peer holds at least the given log index.
func (n *Node) setMatch(peer string, index uint64) {
	n.matchMu.Lock()
	defer n.matchMu.Unlock()
	if index > n.matchIndex[peer] {
		n.matchIndex[peer] = index
	}
}
