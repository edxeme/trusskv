# trusskv

**A tiny distributed key-value store in pure Go.** Raft-style elections,
append-only log replication with quorum commits, a WAL that survives torn
writes, and a 3-node cluster you can run on your laptop in one command.

[![ci](https://github.com/edxeme/trusskv/actions/workflows/ci.yml/badge.svg)](https://github.com/edxeme/trusskv/actions/workflows/ci.yml)

`truss` is named after the structure that carries load across many
members instead of any single one — and built with the same rule: no
single member of the cluster is allowed to matter that much.

## What it does

- **Consensus** — majority-based leader election with randomized
  timeouts, terms, and log-freshness checks (`internal/node`).
- **Replication** — `AppendEntries` with per-peer catch-up; writes are
  fsynced to a leader's WAL and acknowledged by a quorum
  (`internal/node/replicate.go`).
- **Durability** — an append-only WAL framed as
  `[length][crc32][json]` that replays on startup and truncates torn
  tails after a crash (`internal/wal`).
- **Determinism** — a last-writer-wins state machine ordered by
  timestamp, term, then index, so every node converges on the same
  state from the same entries (`internal/store`).
- **Operability** — JSON HTTP API, a `trussctl` CLI, an embedded
  status dashboard, a Docker Compose cluster, and a Kubernetes
  StatefulSet.

Zero external dependencies — the whole thing is the Go standard
library.

## Quickstart

Single node:

```bash
go run ./cmd/trusskv -id=n1 -addr=:7000
```

```bash
go run ./cmd/trussctl -server http://localhost:7000 status
go run ./cmd/trussctl -server http://localhost:7000 set color teal
go run ./cmd/trussctl -server http://localhost:7000 get color
```

Three-node cluster with failover:

```bash
docker compose up --build
# nodes on localhost:7001, 7002, 7003
go run ./cmd/trussctl -server http://localhost:7001 set color teal
open http://localhost:7002   # dashboard on any node
# kill the leader; the cluster re-elects and keeps accepting writes
```

## HTTP API

| Method | Path             | Description                                  |
| ------ | ---------------- | -------------------------------------------- |
| PUT    | `/kv/{key}`      | write (leader only; body is the value)       |
| GET    | `/kv/{key}`      | read (any node; followers may be stale)      |
| DELETE | `/kv/{key}`      | tombstone a key (leader only)                |
| GET    | `/keys`          | live keys known to the contacted node        |
| GET    | `/status`        | id, role, term, leader, log index, uptime    |
| POST   | `/truss/v1/vote`    | peer RPC: consensus vote request          |
| POST   | `/truss/v1/append`  | peer RPC: heartbeat / log replication     |

Non-leaders answer writes with `503 {"error":"not_leader","leader":...}`
so clients can retry against the leader.

## Testing

```bash
go test -race ./...
```

Unit tests cover the state machine, WAL crash recovery, and consensus
handlers (with a scripted transport). The integration test boots a real
3-node cluster on loopback, verifies replication, then kills the leader
and fails over — see `internal/node/integration_test.go`.

## Layout

```
cmd/trusskv/          server binary (one node)
cmd/trussctl/         CLI client
internal/node/        consensus state machine + write path
internal/store/       versioned KV state machine (LWW)
internal/wal/         append-only log with CRC framing
internal/api/         HTTP handlers
internal/httptransport/  node.Transport over JSON HTTP
internal/client/      Go client
internal/web/         embedded dashboard
deploy/k8s/           StatefulSet + headless service
docs/design.md        how it works, guarantees, deliberate simplifications
```

A guided reading order lives in [docs/design.md](docs/design.md).

## Status

Teaching-grade, not production-grade: the simplifications are listed
openly in [docs/design.md](docs/design.md) — no log truncation, no
snapshotting, no membership changes. Everything that *is* implemented
is tested, including failover.

## License

[MIT](LICENSE)

---

*Designed with the same load-path rigor I once applied to concrete —
bricks → bytes.*
