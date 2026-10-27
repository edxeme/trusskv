// Package client provides a small Go client for truss's HTTP API.
package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/edxeme/trusskv/internal/node"
)

// NotLeaderError reports that the contacted node is not the leader and
// names the leader it knows about, if any.
type NotLeaderError struct{ Leader string }

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "truss/client: node is not the leader"
	}
	return fmt.Sprintf("truss/client: node is not the leader; leader is %q", e.Leader)
}

// Client talks to one truss node over HTTP.
type Client struct {
	hc   *http.Client
	base string
}

// New returns a client for the node at base; a nil client gets a default.
func New(base string, hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{Timeout: 5 * time.Second}
	}
	return &Client{hc: hc, base: base}
}

// Set writes key=value through the contacted node. Only the leader accepts
// writes; see NotLeaderError.
func (c *Client) Set(key, value string) error {
	resp, err := c.do(http.MethodPut, "/kv/"+url.PathEscape(key), bytes.NewReader([]byte(value)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return c.check(resp)
}

// Delete removes key through the contacted node.
func (c *Client) Delete(key string) error {
	resp, err := c.do(http.MethodDelete, "/kv/"+url.PathEscape(key), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return c.check(resp)
}

// Get reads key from the contacted node's local state; followers may serve
// stale values.
func (c *Client) Get(key string) (string, bool, error) {
	resp, err := c.do(http.MethodGet, "/kv/"+url.PathEscape(key), nil)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNotFound:
		return "", false, nil
	case http.StatusOK:
		var out kvResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return "", false, fmt.Errorf("truss/client: decode: %w", err)
		}
		return out.Value, true, nil
	default:
		return "", false, c.errorFor(resp)
	}
}

// Keys lists the live keys known to the contacted node.
func (c *Client) Keys() ([]string, error) {
	resp, err := c.do(http.MethodGet, "/keys", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, c.errorFor(resp)
	}
	var out struct {
		Keys []string `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("truss/client: decode: %w", err)
	}
	return out.Keys, nil
}

// Status reads the contacted node's status.
func (c *Client) Status() (node.Status, error) {
	var st node.Status
	resp, err := c.do(http.MethodGet, "/status", nil)
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return st, c.errorFor(resp)
	}
	return st, json.NewDecoder(resp.Body).Decode(&st)
}

func (c *Client) do(method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, c.base+path, body)
	if err != nil {
		return nil, fmt.Errorf("truss/client: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "text/plain")
	}
	return c.hc.Do(req)
}

func (c *Client) check(resp *http.Response) error {
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return c.errorFor(resp)
}

func (c *Client) errorFor(resp *http.Response) error {
	var e errBody
	if err := json.NewDecoder(resp.Body).Decode(&e); err == nil && e.Error == "not_leader" {
		return &NotLeaderError{Leader: e.Leader}
	}
	return fmt.Errorf("truss/client: unexpected status %d", resp.StatusCode)
}

type errBody struct {
	Error  string `json:"error"`
	Leader string `json:"leader"`
}

type kvResponse struct {
	Value string `json:"value"`
}
