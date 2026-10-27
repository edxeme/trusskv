// Package wal implements truss's append-only write-ahead log.
//
// File format: a sequence of records, each framed as
//
//	[4-byte big-endian payload length][4-byte CRC32 of payload][payload]
//
// where the payload is a JSON-encoded Entry. On open, the log is replayed
// and a torn tail (a partially written record, e.g. after a crash mid-append)
// is detected via the length check or CRC mismatch and truncated away.
package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// ErrClosed is returned by Append after Close.
var ErrClosed = errors.New("truss/wal: log is closed")

// maxRecordSize bounds a single record payload (8 MiB). Larger values are
// treated as corruption rather than trusted as a length prefix.
const maxRecordSize = 8 << 20

const (
	lenPrefixSize = 4
	crcSize       = 4
)

// Entry is one replicated operation in the log.
type Entry struct {
	Term  uint64 `json:"term"`
	Index uint64 `json:"index"`
	Op    string `json:"op"` // store.OpPut | store.OpDelete
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
	TS    int64  `json:"ts"` // unix millis
}

// WAL is an append-only log file with in-memory index of replayed entries.
// It is safe for concurrent use.
type WAL struct {
	mu      sync.Mutex
	path    string
	f       *os.File
	entries []Entry
}

// Open opens (creating if needed) the log at path and replays it, repairing
// a torn tail if one is found.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("truss/wal: open %s: %w", path, err)
	}
	w := &WAL{path: path, f: f}
	if err := w.replay(); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

func (w *WAL) replay() error {
	r := bufio.NewReader(w.f)
	var good int64 // bytes of intact records consumed so far
	for {
		var hdr [lenPrefixSize + crcSize]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return w.repair(good) // torn header
		}
		n := binary.BigEndian.Uint32(hdr[:lenPrefixSize])
		sum := binary.BigEndian.Uint32(hdr[lenPrefixSize:])
		if n == 0 || n > maxRecordSize {
			return w.repair(good)
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return w.repair(good) // torn payload
		}
		if crc32.ChecksumIEEE(payload) != sum {
			return w.repair(good) // corrupt payload
		}
		var e Entry
		if err := json.Unmarshal(payload, &e); err != nil {
			return w.repair(good)
		}
		w.entries = append(w.entries, e)
		good += int64(len(hdr)) + int64(n)
	}
}

// repair truncates the file at the last intact record boundary. The bytes
// past it were never acknowledged, so dropping them is always safe.
func (w *WAL) repair(offset int64) error {
	if err := w.f.Truncate(offset); err != nil {
		return fmt.Errorf("truss/wal: repair %s: %w", w.path, err)
	}
	return nil
}

// Append encodes and appends one entry, fsyncs, and updates the in-memory
// index. Entries must arrive with monotonically increasing Index; that is
// the consensus layer's job to guarantee.
func (w *WAL) Append(e Entry) error {
	payload, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("truss/wal: encode entry: %w", err)
	}

	rec := make([]byte, 0, len(payload)+lenPrefixSize+crcSize)
	var hdr [lenPrefixSize + crcSize]byte
	binary.BigEndian.PutUint32(hdr[:lenPrefixSize], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[lenPrefixSize:], crc32.ChecksumIEEE(payload))
	rec = append(rec, hdr[:]...)
	rec = append(rec, payload...)

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return ErrClosed
	}
	if _, err := w.f.Write(rec); err != nil {
		return fmt.Errorf("truss/wal: write %s: %w", w.path, err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("truss/wal: sync %s: %w", w.path, err)
	}
	w.entries = append(w.entries, e)
	return nil
}

// Last returns the index and term of the newest entry, or (0, 0) if empty.
func (w *WAL) Last() (index, term uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.entries) == 0 {
		return 0, 0
	}
	e := w.entries[len(w.entries)-1]
	return e.Index, e.Term
}

// After returns up to limit entries with Index greater than minIndex,
// oldest first. limit <= 0 means no limit.
func (w *WAL) After(minIndex uint64, limit int) []Entry {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := []Entry{}
	for i := range w.entries {
		if w.entries[i].Index > minIndex {
			out = append(out, w.entries[i:]...)
			break
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// Len reports the number of entries in the log.
func (w *WAL) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.entries)
}

// Close syncs and closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}
