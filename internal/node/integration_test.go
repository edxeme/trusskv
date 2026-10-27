package node_test

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/edxeme/trusskv/internal/api"
	"github.com/edxeme/trusskv/internal/client"
	"github.com/edxeme/trusskv/internal/httptransport"
	"github.com/edxeme/trusskv/internal/node"
	"github.com/edxeme/trusskv/internal/store"
	"github.com/edxeme/trusskv/internal/wal"
)

// cluster is a real 3-node truss cluster on the loopback interface: nodes
// speak the production HTTP transport through actual TCP listeners.
type cluster struct {
	t       *testing.T
	urls    []string
	nodes   []*node.Node
	clients []*client.Client
}

func startCluster(t *testing.T, size int) *cluster {
	t.Helper()

	// Reserve listeners first so peer URLs are known before nodes exist.
	listeners := make([]net.Listener, size)
	urls := make([]string, size)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[i] = l
		urls[i] = "http://" + l.Addr().String()
	}

	tr := httptransport.New(&http.Client{Timeout: 2 * time.Second})
	c := &cluster{t: t, urls: urls}
	for i := 0; i < size; i++ {
		id := fmt.Sprintf("n%d", i)
		logFile, err := wal.Open(filepath.Join(t.TempDir(), id+".wal"))
		if err != nil {
			t.Fatalf("open wal: %v", err)
		}
		n, err := node.New(node.Config{
			ID:                 id,
			Advertise:          urls[i],
			Peers:              urls,
			Transport:          tr,
			Store:              store.New(),
			Log:                logFile,
			Heartbeat:          40 * time.Millisecond,
			ElectionTimeoutMin: 80 * time.Millisecond,
			ElectionTimeoutMax: 160 * time.Millisecond,
			RPCTimeout:         500 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("new node: %v", err)
		}

		apiServer := &api.Server{Node: n}
		srv := &http.Server{Handler: apiServer.Handler()} //nolint:gosec // loopback test server
		go func(l net.Listener) { _ = srv.Serve(l) }(listeners[i])

		c.nodes = append(c.nodes, n)
		c.clients = append(c.clients, client.New(urls[i], &http.Client{Timeout: 2 * time.Second}))
		t.Cleanup(func() {
			srv.Close()
			n.Stop()
			logFile.Close()
		})
	}
	for _, n := range c.nodes {
		n.Start()
	}
	return c
}

// leaderIndex returns the index of the current leader, or -1 if none.
func (c *cluster) leaderIndex() int {
	for i, n := range c.nodes {
		if ok, _ := n.IsLeader(); ok {
			return i
		}
	}
	return -1
}

func (c *cluster) waitLeader(timeout time.Duration) int {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if i := c.leaderIndex(); i >= 0 {
			return i
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("timed out waiting for a leader")
	return -1
}

func TestClusterReplicatesAndFailsOver(t *testing.T) {
	c := startCluster(t, 3)

	// A leader emerges and a client write succeeds through the HTTP API.
	leader := c.waitLeader(15 * time.Second)
	if err := c.clients[leader].Set("color", "teal"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if v, ok, err := c.clients[leader].Get("color"); err != nil || !ok || v != "teal" {
		t.Fatalf("get = %q, %v, %v; want teal", v, ok, err)
	}

	// Quorum means at least one follower holds the entry; verify it can be
	// read from that follower's own HTTP API.
	replicas := 0
	for i := range c.nodes {
		if i == leader {
			continue
		}
		if v, ok, err := c.clients[i].Get("color"); err == nil && ok && v == "teal" {
			replicas++
		}
	}
	if replicas == 0 {
		t.Fatal("no follower holds the replicated entry")
	}

	// Kill the leader: the surviving majority must elect a new one, and the
	// cluster must accept writes again.
	killed := leader
	deadline := time.Now().Add(15 * time.Second)
	c.nodes[killed].Stop()

	var newLeader int
	for {
		newLeader = c.leaderIndex()
		if newLeader >= 0 && newLeader != killed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for failover")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := c.clients[newLeader].Set("after", "failover"); err != nil {
		t.Fatalf("set after failover: %v", err)
	}
	if v, ok, err := c.clients[newLeader].Get("after"); err != nil || !ok || v != "failover" {
		t.Fatalf("get after failover = %q, %v, %v; want failover", v, ok, err)
	}

	// Reads and deletes keep working; the log grew through both terms.
	if err := c.clients[newLeader].Delete("color"); err != nil {
		t.Fatalf("delete after failover: %v", err)
	}
	if _, ok, _ := c.clients[newLeader].Get("color"); ok {
		t.Fatal("color must be gone after delete")
	}
}

func TestClusterRejectsWritesOnFollower(t *testing.T) {
	c := startCluster(t, 3)
	c.waitLeader(15 * time.Second)

	// Every non-leader must refuse writes and hint at the leader it knows.
	leader := c.leaderIndex()
	rejections := 0
	for i := range c.nodes {
		if i == leader {
			continue
		}
		err := c.clients[i].Set("k", "v")
		if err == nil {
			t.Fatalf("follower n%d accepted a write", i)
		}
		var nle *client.NotLeaderError
		if !errors.As(err, &nle) {
			t.Fatalf("follower error = %v; want NotLeaderError", err)
		}
		if nle.Leader != fmt.Sprintf("n%d", leader) {
			t.Fatalf("leader hint = %q; want n%d", nle.Leader, leader)
		}
		rejections++
	}
	if rejections != 2 {
		t.Fatalf("rejections = %d; want 2", rejections)
	}
}
