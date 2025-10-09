package main

import "sync"

type Store struct {
	kv    map[string][]byte
	lasti uint64
	mu    sync.Mutex
}
