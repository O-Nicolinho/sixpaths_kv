package sixpaths_kvs

func IncExec()  {}
func IncPut()   {}
func IncDel()   {}
func IncDedup() {}

type MetricsSnapshot struct {
	ExecTotal uint64 `json:"exec_total"`
	PutTotal  uint64 `json:"put_total"`
	DelTotal  uint64 `json:"del_total"`
	DedupHits uint64 `json:"dedup_hits"`
}

func Snapshot() MetricsSnapshot {
	return MetricsSnapshot{}
}
