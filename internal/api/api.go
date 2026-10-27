// Package api exposes truss's HTTP surface: peer-to-peer consensus RPCs,
// a client-facing KV API, and status endpoints.
package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/edxeme/trusskv/internal/node"
	"github.com/edxeme/trusskv/internal/web"
)

// maxValueSize bounds a single stored value (1 MiB).
const maxValueSize = 1 << 20

// Server wires a node into HTTP handlers.
type Server struct {
	Node *node.Node
}

// Handler returns the full HTTP surface of a node.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /truss/v1/vote", s.handleVote)
	mux.HandleFunc("POST /truss/v1/append", s.handleAppend)
	mux.HandleFunc("PUT /kv/{key}", s.handlePut)
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("DELETE /kv/{key}", s.handleDelete)
	mux.HandleFunc("GET /keys", s.handleKeys)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /{$}", s.handleHome)
	return mux
}

func (s *Server) handleVote(w http.ResponseWriter, r *http.Request) {
	var req node.VoteRequest
	if !decode(w, r, &req) {
		return
	}
	writeJSON(w, http.StatusOK, s.Node.HandleVote(r.Context(), req))
}

func (s *Server) handleAppend(w http.ResponseWriter, r *http.Request) {
	var req node.AppendRequest
	if !decode(w, r, &req) {
		return
	}
	writeJSON(w, http.StatusOK, s.Node.HandleAppend(r.Context(), req))
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxValueSize))
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, errBody{Error: "payload too large"})
		return
	}
	if err := s.Node.Put(r.PathValue("key"), body); err != nil {
		writeNodeError(w, s.Node, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	rec, ok := s.Node.Get(key)
	if !ok {
		writeJSON(w, http.StatusNotFound, errBody{Error: "not_found"})
		return
	}
	writeJSON(w, http.StatusOK, kvResponse{
		Key:       key,
		Value:     string(rec.Value),
		Term:      rec.Term,
		Index:     rec.Index,
		Timestamp: rec.Timestamp,
	})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.Node.Delete(r.PathValue("key")); err != nil {
		writeNodeError(w, s.Node, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleKeys(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, keysResponse{Keys: s.Node.Keys()})
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Node.Status())
}

func (s *Server) handleHome(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	web.Render(w, s.Node.Status(), s.Node.Snapshot(), time.Now())
}

type errBody struct {
	Error  string `json:"error"`
	Leader string `json:"leader,omitempty"`
}

type kvResponse struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Term      uint64 `json:"term"`
	Index     uint64 `json:"index"`
	Timestamp int64  `json:"timestamp"`
}

type keysResponse struct {
	Keys []string `json:"keys"`
}

// writeNodeError maps node errors onto HTTP responses. A not-leader error
// carries the leader the node currently knows about, so clients can retry.
func writeNodeError(w http.ResponseWriter, n *node.Node, err error) {
	switch {
	case errors.Is(err, node.ErrNotLeader):
		_, leader := n.IsLeader()
		writeJSON(w, http.StatusServiceUnavailable, errBody{Error: "not_leader", Leader: leader})
	case errors.Is(err, node.ErrNoQuorum):
		writeJSON(w, http.StatusServiceUnavailable, errBody{Error: "no_quorum"})
	default:
		writeJSON(w, http.StatusInternalServerError, errBody{Error: err.Error()})
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(io.LimitReader(r.Body, maxValueSize)).Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody{Error: "bad request"})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Headers are already written; nothing to recover.
		return
	}
}
