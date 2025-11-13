package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/O-Nicolinho/sixpaths_kv/internal/sixpaths_kvs"
)

func main() {
	id := flag.String("id", "", "node ID (n1..n6)")
	flag.Parse()

	if *id == "" {
		log.Fatalf("must provide -id (n1..n6)")
	}

	// Look up our config and the full cluster.
	cfg, all, err := sixpaths_kvs.ConfigForID(*id)
	if err != nil {
		log.Fatalf("ConfigForID: %v", err)
	}

	// Boot node (WAL open + replay -> Store), with ID + peers filled in.
	node, err := sixpaths_kvs.OpenClusterNode(cfg, all)
	if err != nil {
		log.Fatalf("OpenClusterNode failed: %v", err)
	}
	defer func() {
		if err := node.Close(); err != nil {
			log.Printf("node.Close error: %v", err)
		}
	}()

	// Start HTTP server on cfg.ClientAddr
	srv := sixpaths_kvs.NewHTTPServer(node, cfg.ClientAddr)
	go func() {
		log.Printf("serving at %s (id=%s data=%s)", cfg.ClientAddr, cfg.ID, cfg.DataDir)
		if err := srv.Start(); err != nil {
			log.Printf("server exited: %v", err)
		}
	}()

	// Graceful shutdown on SIGINT/SIGTERM
	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)
	<-sigch

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}
	log.Printf("adieu")
}
