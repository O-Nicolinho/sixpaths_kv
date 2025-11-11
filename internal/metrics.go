package sixpaths_kvs

import "sync/atomic"

var (
	execTotal uint64
	putTotal  uint64
	delTotal  uint64
	dedupHits uint64
)

func IncExec() {
	atomic.AddUint64(&execTotal, 1)
}

func IncPut() {
	atomic.AddUint64(&putTotal, 1)
}
func IncDel() {
	atomic.AddUint64(&delTotal, 1)
}
func IncDedup() {
	atomic.AddUint64(&dedupHits, 1)
}

type MetricsSnapshot struct {
	ExecTotal uint64 `json:"exec_total"`
	PutTotal  uint64 `json:"put_total"`
	DelTotal  uint64 `json:"del_total"`
	DedupHits uint64 `json:"dedup_hits"`
}

func Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		ExecTotal: atomic.LoadUint64(&execTotal),
		PutTotal:  atomic.LoadUint64(&putTotal),
		DelTotal:  atomic.LoadUint64(&delTotal),
		DedupHits: atomic.LoadUint64(&dedupHits),
	}
}
