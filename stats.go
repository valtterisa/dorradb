package dorradb

import "time"

// Stats is a snapshot of engine counters for ops/debugging (CLI status).
type Stats struct {
	Path            string
	Seq             uint64 // last assigned mutation sequence number
	LiveKeys        int
	MainBytes       int64 // size of *.dorradb
	WALBytes        int64 // size of *-wal
	ApproxDeadBytes int64 // roughly main size minus live record bytes (grows until checkpoint)
	LastCheckpoint  time.Time
}
