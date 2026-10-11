package dorradb

import "time"

const (
	DefaultWALCheckpointBytes    int64 = 8 << 20
	DefaultWALCheckpointInterval       = 30 * time.Second
	DefaultGroupCommitMaxOps           = 64
	DefaultGroupCommitMaxWait          = 2 * time.Millisecond
	DefaultMaxKeyBytes                 = 1 << 10
	DefaultMaxValueBytes               = 4 << 20
)

type Options struct {
	Path                  string
	WALCheckpointBytes    int64
	WALCheckpointInterval time.Duration
	SyncWAL               *bool
	GroupCommitMaxOps     int
	GroupCommitMaxWait    time.Duration
	MaxKeyBytes           int
	MaxValueBytes         int
}

func Bool(v bool) *bool { return &v }

func (o Options) withDefaults() Options {
	if o.WALCheckpointBytes <= 0 {
		o.WALCheckpointBytes = DefaultWALCheckpointBytes
	}
	if o.WALCheckpointInterval <= 0 {
		o.WALCheckpointInterval = DefaultWALCheckpointInterval
	}
	if o.GroupCommitMaxOps <= 0 {
		o.GroupCommitMaxOps = DefaultGroupCommitMaxOps
	}
	if o.GroupCommitMaxWait <= 0 {
		o.GroupCommitMaxWait = DefaultGroupCommitMaxWait
	}
	if o.MaxKeyBytes <= 0 {
		o.MaxKeyBytes = DefaultMaxKeyBytes
	}
	if o.MaxValueBytes <= 0 {
		o.MaxValueBytes = DefaultMaxValueBytes
	}
	if o.SyncWAL == nil {
		o.SyncWAL = Bool(true)
	}
	return o
}

func (o Options) syncWAL() bool {
	if o.SyncWAL == nil {
		return true
	}
	return *o.SyncWAL
}
