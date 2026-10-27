// Command trusskv runs a single truss node.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/edxeme/trusskv/internal/api"
	"github.com/edxeme/trusskv/internal/httptransport"
	"github.com/edxeme/trusskv/internal/node"
	"github.com/edxeme/trusskv/internal/store"
	"github.com/edxeme/trusskv/internal/wal"
)

func main() {
	id := flag.String("id", "", "node id, unique within the cluster (required)")
	addr := flag.String("addr", ":7000", "listen address")
	advertise := flag.String("advertise", "", "URL peers use to reach this node (default: http://localhost<addr>)")
	peers := flag.String("peers", "", "comma-separated peer base URLs")
	dataDir := flag.String("data", "data", "data directory for the write-ahead log")
	flag.Parse()

	if *id == "" {
		log.Fatal("trusskv: -id is required")
	}
	if err := run(*id, *addr, *advertise, *peers, *dataDir); err != nil {
		log.Fatal(err)
	}
}

func run(id, addr, advertise, peers, dataDir string) error {
	if advertise == "" {
		advertise = "http://localhost" + addr
	}
	peerURLs := splitPeers(peers)

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	logFile, err := wal.Open(filepath.Join(dataDir, id+".wal"))
	if err != nil {
		return err
	}
	defer logFile.Close()

	n, err := node.New(node.Config{
		ID:        id,
		Advertise: advertise,
		Peers:     peerURLs,
		Transport: httptransport.New(&http.Client{Timeout: 2 * time.Second}),
		Store:     store.New(),
		Log:       logFile,
	})
	if err != nil {
		return err
	}
	n.Start()
	defer n.Stop()

	apiServer := &api.Server{Node: n}
	srv := &http.Server{
		Addr:              addr,
		Handler:           apiServer.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	log.Printf("trusskv: node %s listening on %s (peers: %v)", id, addr, peerURLs)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
		log.Printf("trusskv: node %s shut down", id)
		return nil
	}
}

func splitPeers(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
