package dorradb

import (
	"time"

	"dorradb/internal/engine"
)

// Options configures Open. Pass only fields you want to change; zero values keep
// the defaults from DefaultOptions.
//
//	db, err := dorradb.Open("app.dorradb", dorradb.Options{
//	    WALCheckpointBytes: 32 << 20, // 32 MiB
//	    DisableSync:        true,     // skip WAL fsync (less durable)
//	})
type Options struct {
	// WALCheckpointBytes triggers a checkpoint when the WAL has grown by this
	// many bytes since the last checkpoint. Default: 8 MiB.
	WALCheckpointBytes int64

	// WALCheckpointInterval triggers a checkpoint after this duration with
	// mutations since the last checkpoint. Default: 30s.
	WALCheckpointInterval time.Duration

	// DisableSync skips fsync of the WAL on each group commit. Faster writes,
	// but a crash may lose recently acknowledged mutations. Default: false (sync on).
	DisableSync bool

	// GroupCommitMaxOps is how many Sets/Deletes may share one WAL sync.
	// Default: 64.
	GroupCommitMaxOps int

	// GroupCommitMaxWait is how long the writer waits to gather more mutations
	// before syncing. Default: 2ms.
	GroupCommitMaxWait time.Duration
}

// DefaultOptions returns the defaults used when Open is called with no Options.
func DefaultOptions() Options {
	e := engine.DefaultOptions()
	return Options{
		WALCheckpointBytes:    e.WALCheckpointBytes,
		WALCheckpointInterval: e.WALCheckpointInterval,
		DisableSync:           !e.SyncWAL,
		GroupCommitMaxOps:     e.GroupCommitMaxOps,
		GroupCommitMaxWait:    e.GroupCommitMaxWait,
	}
}

func (o Options) toEngine() engine.Options {
	e := engine.DefaultOptions()
	if o.WALCheckpointBytes != 0 {
		e.WALCheckpointBytes = o.WALCheckpointBytes
	}
	if o.WALCheckpointInterval != 0 {
		e.WALCheckpointInterval = o.WALCheckpointInterval
	}
	if o.DisableSync {
		e.SyncWAL = false
	}
	if o.GroupCommitMaxOps != 0 {
		e.GroupCommitMaxOps = o.GroupCommitMaxOps
	}
	if o.GroupCommitMaxWait != 0 {
		e.GroupCommitMaxWait = o.GroupCommitMaxWait
	}
	return e
}
