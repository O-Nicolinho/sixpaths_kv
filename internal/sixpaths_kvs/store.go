package sixpaths_kvs

import (
	"errors"
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

func (store *Store) Get(key string) ([]byte, error) {

	// TODO: Consider a change from Lock/Unlock to a READ lock/unlock to maximize concurrency
	store.mu.Lock()
	defer store.mu.Unlock()

	val, ok := store.kv[key]

	if !ok {
		return nil, errors.New("error: No value at specificed key in map.")
	}
	//make a copy of val
	valcopy := append([]byte{}, val...)

	return valcopy, nil
}
