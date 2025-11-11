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
	addr := flag.String("addr", ":8080", "HTTP listen address")
	data := flag.String("data", "./data", "data directory")
	flag.Parse()

	// Boot node (WAL open + replay -> Store)
	node, err := sixpaths_kvs.OpenNode(*data)
	if err != nil {
		log.Fatalf("OpenNode failed: %v", err)
	}
	defer func() {
		if err := node.Close(); err != nil {
			log.Printf("node.Close error: %v", err)
		}
	}()

	// Start HTTP server
	srv := sixpaths_kvs.NewHTTPServer(node, *addr)
	go func() {
		log.Printf("serving at %s (data=%s)", *addr, *data)
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
	log.Printf("bye")
}
