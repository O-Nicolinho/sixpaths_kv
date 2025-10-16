package sixpaths_kvs

import (
	"bufio"
	"os"
	"path/filepath"
)

type WAL struct {
	f      *os.File
	path   string
	offset int64
	bw *bufio.Writer
	hdrLen int
}



type Record struct {
	LogIndex uint64
	Cmd      Command
}

func NewWAL(path string) (*WAL, error) {
	var walHeader = []byte("WALv1-BE\x00")

	dir := filepath.Dir(path)
	err := os.MkdirAll(dir, 0o755)
	// checks whether directory specified by path exists.
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.0_RDWR|os.0_CREATE, 0o644)

	if err != nil {
		return nil, err
	}

}
