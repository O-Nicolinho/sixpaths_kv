package sixpaths_kvs

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Node struct {
	wal     *WAL
	store   *Store
	last    uint64
	mu      sync.Mutex
	dataDir string
}

func OpenNode(dataDir string) (*Node, error) {

	// we check whether the dir at dataDir exists
	info, err := os.Stat(dataDir)

	// we check that dataDir is a directory and not a file
	if info.IsDir() == false {
		return nil, fmt.Errorf("error: dataDir is a file, not a directory: %w", err)
	}

	// if it doesn't exist, we create it.
	if os.IsNotExist(err) {
		err = os.MkdirAll(dataDir, 0o755)
		if err != nil {
			return nil, fmt.Errorf("error: OpenNode() failure, unable to create directory: %w", err)
		}
	}
	// any other errors: just return the error
	if err != nil {
		return nil, err
	}

	// filepath dataDir/wal
	pth := filepath.Join(dataDir, "wal")

	// we create a new WAL using the path dataDir/wal
	nwal, err := NewWAL(pth)
	if err != nil {
		return nil, fmt.Errorf("error: OpenNode() failure, unable to create WAL: %w", err)
	}

	// now we use ReplayAll to get a slice of Records
	// this will allow us to use these records to recreate the KV store
	recs, lastidx, err := nwal.ReplayAll()
	if err != nil {
		// on failure we close the WAL
		nwal.Close()
		return nil, err
	}

	// now we create a new KV store
	nstore, err := NewStore()
	if err != nil {
		// on failure we close the WAL
		nwal.Close()
		return nil, err
	}

	// now we iterate over our records and Apply() them sequentially
	for _, rec := range recs {
		_, err = nstore.Apply(rec.Cmd, rec.LogIndex)
		if err != nil {
			// on failure we close the WAL
			nwal.Close()
			return nil, fmt.Errorf("error Applying: %w", err)
		}
	}

	newNode := Node{
		wal:     nwal,
		store:   nstore,
		last:    lastidx,
		mu:      sync.Mutex{},
		dataDir: dataDir,
	}

	return &newNode, nil
}

func (n *Node) Close() error {

	// check that node and wal are not nil
	if n == nil || n.wal == nil {
		return nil
	}

	//attempt to close the WAL
	err := n.wal.Close()

	// on failure, just return the error
	if err != nil {
		return err
	}
	//on success, return nil
	return nil
}

func (n *Node) Exec(cmd Command) (ApplyResult, error) {
	// we take the lock to keep the writes serial
	n.mu.Lock()
	defer n.mu.Unlock()
	// we build the skeleton of the ApplyResult we're gonna return

	// we check if this request is a duplicate (by comparing Seqs)
	n.store.mu.Lock()
	if n.store.dedupMap[cmd.ClientID].seq >= cmd.Seq {
		defer n.store.mu.Unlock()
		return n.store.dedupMap[cmd.ClientID].result, nil
	}
	n.store.mu.Unlock()

	// we check if the cmd type is valid
	if !validType(cmd.Instruct) {

		return ApplyResult{}, fmt.Errorf("error: invalid command")
	}

	// we get the index for the next command application
	nextIdx := n.last + 1

	// we build the record for our WAL
	appRec := Record{
		LogIndex: nextIdx,
		Cmd:      cmd,
	}
	// we get the encoded record to add to our WAL
	err := n.wal.Append(&appRec)
	if err != nil {
		return ApplyResult{}, err
	}

	ap, err := n.store.Apply(cmd, nextIdx)
	if err != nil {
		return ap, err
	}

	n.last = nextIdx

	return ap, nil
}

func (n *Node) Get(key string) ([]byte, error) {
	return n.store.Get(key)
}
