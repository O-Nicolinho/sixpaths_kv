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
