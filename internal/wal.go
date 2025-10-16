package sixpaths_kvs

import "os"

type WAL struct {
	f      *os.File
	path   string
	offset int64
}

type Record struct {
	LogIndex uint64
	Cmd      Command
}

func NewWal(path string) (WAL, err) {

}
