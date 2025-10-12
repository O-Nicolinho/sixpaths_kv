package sixpaths_kvs

import (
	"sync"
)

type Store struct {
	kv       map[string][]byte
	lastlogi uint64
	mu       sync.Mutex
	dedupMap map[string]Dedup // map where key=clientID string, value=dedup
}

type Dedup struct {
	seq    uint64
	result ApplyResult
}

func NewStore() (*Store, error) {
	var st Store = Store{
		kv:       make(map[string][]byte),
		dedupMap: make(map[string]Dedup),
	}
	
	return &st, nil
}
