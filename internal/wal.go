package sixpaths_kvs

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type WAL struct {
	f      *os.File
	path   string
	offset int64
	bw     *bufio.Writer
	hdrLen int
}

type Record struct {
	LogIndex uint64
	Cmd      Command
}

func NewWAL(path string) (*WAL, error) {
	var walHeader = []byte("WALv1-BE\x00")

	// we create the skeleton of the WAL we're going to return
	var newWAL *WAL = new(WAL)

	dir := filepath.Dir(path)
	err := os.MkdirAll(dir, 0o755)
	// checks whether directory specified by path exists.
	if err != nil {
		return nil, err
	}

	// if the file doesn't exist, we create one.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)

	if err != nil {
		return nil, err
	}

	// big buffer
	bw := bufio.NewWriterSize(f, 64<<10)

	info, err := f.Stat()

	if err != nil {
		return nil, err
	}

	// Check if the file is empty
	if info.Size() <= 0 {
		// If it is, we write the header to it.
		offset, err := f.Write(walHeader)

		if err != nil {
			return nil, err
		}

		err = f.Sync()
		if err != nil {
			return nil, err
		}
		// Update the offset for our new file.
		newWAL.offset = int64(offset)

	} else {
		_, err = f.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}

		hdr := make([]byte, len(walHeader))
		_, err = io.ReadFull(f, hdr)
		if err != nil {
			return nil, err
		}

		if !bytes.Equal(hdr, walHeader) {
			return nil, fmt.Errorf("bad WAL header: expected %q", walHeader)
		}
		// placeholder, offset = curr file size
		newWAL.offset = info.Size()

		// put cursor at the end to facilitate appends
		_, err = f.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, err
		}
	}

	// Fill in our new WAL struct
	newWAL.f = f
	newWAL.path = path
	newWAL.bw = bw
	newWAL.hdrLen = len(walHeader)

	return newWAL, nil
}
