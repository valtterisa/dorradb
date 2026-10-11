package dorradb

// Stats is a snapshot of engine counters for ops/debugging (CLI status).
type Stats struct {
	Path              string
	Sequence          uint64
	CommittedSequence uint64
	PinnedReaders     int
	LiveKeys          int
	MainBytes         int64
	WALBytes          int64
	ApproxDeadBytes   int64
}
