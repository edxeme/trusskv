# truss design notes

truss is a teaching-grade distributed key-value store. This document
explains how it works, what it guarantees, and — just as important —
which corners were cut on purpose so the code stays readable in one
sitting.

## Architecture

```
            ┌────────────────────────────────────────────┐
            │                  node                      │
            │  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
 client ───►│  │   api    │──►│  store   │◄─│   wal    │  │
  HTTP      │  └────┬─────┘  └──────────┘  └────▲─────┘  │
            │       │        (state machine)    (log)     │
            │  ┌────▼───────────────────────────┴─────┐  │
            │  │        consensus state machine       │  │
            │  └────┬───────────────────────┬─────────┘  │
            └───────┼───────────────────────┼────────────┘
              vote/append                   vote/append
                    ▼                       ▼
                 peer n2                  peer n3
```

- `internal/node` — the consensus state machine: terms, majority-based
  elections, heartbeat-driven replication with per-peer catch-up, and
  quorum-gated writes.
- `internal/store` — the applied state: a versioned map with
  last-writer-wins ordering (timestamp, then term, then index).
- `internal/wal` — the append-only log: length-prefixed CRC32-framed
  records, replayed on startup, torn tails truncated.
- `internal/api` — HTTP surface: peer RPCs under `/truss/v1/`, the
  client KV API under `/kv/`, and status endpoints.
- `internal/httptransport` — `node.Transport` over JSON HTTP.
- `internal/client` + `cmd/trussctl` — Go client and CLI.
- `internal/web` — the embedded status dashboard.

## Protocol

### Elections

Each node runs a randomized election timeout in
`[ElectionTimeoutMin, ElectionTimeoutMax]`. When the timeout elapses
without hearing from a leader, a node increments the term, votes for
itself, and fans out `RequestVote` to every peer. A candidate wins with
a majority (self-vote included) and immediately heartbeats to establish
authority. Votes are granted at most once per term, and only to
candidates whose log tail is at least as long (by term, then index).

Randomized timeouts are what makes split votes self-resolving: two
campaigning nodes are unlikely to keep timing out simultaneously.

### Replication

Only the leader accepts writes. A write:

1. is appended to the leader's WAL (fsynced),
2. is applied to the leader's store,
3. fans out `AppendEntries` carrying every entry the peer is missing
   (bounded by `maxBatch`), in parallel, under an RPC deadline,
4. succeeds only when a majority of peers acknowledges holding the
   entry in the leader's term; otherwise the client gets `ErrNoQuorum`.

Heartbeats are empty `AppendEntries`, but they piggyback catch-up: the
leader tracks each peer's `matchIndex` and resends everything after it,
so a follower that restarts empty catches up without special-casing.
Followers apply entries eagerly and skip duplicate deliveries (index
already present).

### Failover

Followers that stop hearing from the leader campaign after their
timeout. The freshness check in vote granting biases elections toward
the most complete log. `internal/node/integration_test.go` kills the
leader of a live 3-node cluster and verifies re-election plus continued
writes.

## Consistency model

truss gives you **leader-local linearizability for acknowledged writes**
and **eventual consistency for reads**:

- A write that returns `nil` is durable on the leader and acknowledged
  by a majority. A write that returns `ErrNoQuorum` is still durable on
  the leader and possibly on some followers — the caller knows the
  write *may* have taken effect but is not guaranteed to have quorum.
- Reads are served from the contacted node's local store. Reading from
  a follower may return stale data; reading from the leader may trail
  in-flight replication by design.

## Deliberate simplifications

These are the corners cut to keep truss readable. Each one is a real
design decision worth understanding — and a good exercise to fix:

1. **No log truncation.** `AppendEntries` never rewrites the follower
   log. Raft's log backtracking (conflict resolution when a new leader
   has a shorter log) is absent. Elections mitigate this with the
   freshness check, but divergent uncommitted suffixes are possible
   after a leader crash mid-term.
2. **Followers apply eagerly.** Followers apply entries when they
   receive them, not when the leader commits them. Combined with (1),
   follower state can temporarily diverge in pathological failure
   modes rather than roll back.
3. **Quorum gates the response, not visibility.** The leader applies
   before replication completes. truss is not a strict quorum store.
4. **No snapshotting.** The WAL grows forever and is replayed fully on
   startup; catch-up streams from the leader's log on every heartbeat
   gap. Fine for teaching; not for a long-lived production cluster.
5. **Single JSON HTTP transport.** Peer RPCs are JSON over HTTP/1.1
   with per-call deadlines. No gRPC, no streaming, no connection
   pooling beyond `net/http`'s defaults.
6. **No membership changes.** Cluster size is fixed at boot via
   `-peers`. Adding a peer means restarting the cluster.

## Reading guide

Suggested order for a first pass:

1. `internal/store/store.go` — 100 lines; the deterministic state
   machine and its LWW ordering rule.
2. `internal/wal/wal.go` — the framing format and torn-tail recovery.
3. `internal/node/node.go` — terms, roles, elections, and the RPC
   handlers; the state machine fits on two screens.
4. `internal/node/replicate.go` — the write path, quorum fan-out, and
   per-peer catch-up.
5. `internal/node/integration_test.go` — a real cluster on loopback
   failing over in under half a second.

## Operations

- **Local:** `docker compose up --build` brings up a 3-node cluster on
  `localhost:7001-7003`.
- **Kubernetes:** `deploy/k8s` runs a 3-replica StatefulSet with peer
  discovery over the headless service DNS; peers and identity are
  wired via the pod name. The WAL lives on a per-pod PVC.

Build the image first (`docker build -t trusskv:local .`), then apply
the manifests; scale beyond 3 replicas by extending `-peers` on every
node (see simplification 6).
