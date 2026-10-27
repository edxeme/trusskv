// Package httptransport implements node.Transport over JSON HTTP calls.
package httptransport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/edxeme/trusskv/internal/node"
)

// Transport posts consensus RPCs to peer base URLs.
type Transport struct {
	hc *http.Client
}

// New returns a Transport; a nil client gets a fresh one.
func New(hc *http.Client) *Transport {
	if hc == nil {
		hc = &http.Client{}
	}
	return &Transport{hc: hc}
}

// RequestVote posts a vote request to the peer.
func (t *Transport) RequestVote(ctx context.Context, peer string, req node.VoteRequest) (node.VoteResponse, error) {
	var out node.VoteResponse
	return out, t.post(ctx, peer+"/truss/v1/vote", req, &out)
}

// AppendEntries posts an AppendEntries request to the peer.
func (t *Transport) AppendEntries(ctx context.Context, peer string, req node.AppendRequest) (node.AppendResponse, error) {
	var out node.AppendResponse
	return out, t.post(ctx, peer+"/truss/v1/append", req, &out)
}

func (t *Transport) post(ctx context.Context, url string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("truss/transport: encode: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("truss/transport: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.hc.Do(req)
	if err != nil {
		return fmt.Errorf("truss/transport: %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("truss/transport: %s: status %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("truss/transport: %s: decode: %w", url, err)
	}
	return nil
}
