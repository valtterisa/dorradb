package dorradb

import "time"

// Stats is a snapshot of engine counters for ops/debugging (CLI status).
type Stats struct {
	Path            string
	Seq             uint64
	LiveKeys        int
	MainBytes       int64
	WALBytes        int64
	ApproxDeadBytes int64
	LastCheckpoint  time.Time
}
