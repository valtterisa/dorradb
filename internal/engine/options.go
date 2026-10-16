package engine

import "time"

// Options tunes durability and background checkpointing.
// Prefer DefaultOptions or Normalize; zero values are filled by Normalize
// except VacuumDeadBytes (0 means disabled) and SyncWAL.
type Options struct {
	// WALCheckpointBytes triggers a checkpoint when WAL growth since the last
	// checkpoint reaches this many bytes. Default: 8 MiB.
	WALCheckpointBytes int64

	// WALCheckpointInterval triggers a checkpoint after this much time has
	// passed with mutations since the last checkpoint. Default: 30s.
	WALCheckpointInterval time.Duration

	// SyncWAL fsyncs the WAL on each group commit. Default: true.
	SyncWAL bool

	// GroupCommitMaxOps is the max mutations coalesced into one WAL sync.
	// Default: 64.
	GroupCommitMaxOps int

	// GroupCommitMaxWait is how long the writer waits to gather more mutations
	// before syncing. Default: 2ms.
	GroupCommitMaxWait time.Duration

	// CachePages is the fixed buffer-pool frame count. Default: 256. Minimum: 2.
	CachePages int

	// VacuumDeadBytes triggers vacuum after checkpoint when ApproxDeadBytes
	// reaches this size. Default: 8 MiB. 0 disables automatic vacuum.
	VacuumDeadBytes int64
}

// DefaultOptions returns the production defaults used by Open.
func DefaultOptions() Options {
	return Options{
		WALCheckpointBytes:    8 << 20,
		WALCheckpointInterval: 30 * time.Second,
		SyncWAL:               true,
		GroupCommitMaxOps:     64,
		GroupCommitMaxWait:    2 * time.Millisecond,
		CachePages:            256,
		VacuumDeadBytes:       8 << 20,
	}
}

// Normalize fills zero numeric/duration fields with defaults.
// SyncWAL is left as-is. VacuumDeadBytes 0 stays 0 (disabled) only when the
// caller started from a zero Options and explicitly wants disable — prefer
// DefaultOptions then set VacuumDeadBytes = 0.
func (o Options) Normalize() Options {
	d := DefaultOptions()
	if o.WALCheckpointBytes == 0 {
		o.WALCheckpointBytes = d.WALCheckpointBytes
	}
	if o.WALCheckpointInterval == 0 {
		o.WALCheckpointInterval = d.WALCheckpointInterval
	}
	if o.GroupCommitMaxOps == 0 {
		o.GroupCommitMaxOps = d.GroupCommitMaxOps
	}
	if o.GroupCommitMaxWait == 0 {
		o.GroupCommitMaxWait = d.GroupCommitMaxWait
	}
	if o.CachePages == 0 {
		o.CachePages = d.CachePages
	}
	// VacuumDeadBytes: 0 means disabled; negative treated as disabled.
	if o.VacuumDeadBytes < 0 {
		o.VacuumDeadBytes = 0
	}
	return o
}
