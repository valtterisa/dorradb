// Package dorradb is an embedded key-value store with a WAL and checkpoints.
//
// Open a database file (created if missing):
//
//	db, err := dorradb.Open("app.dorradb")
//
// Tune durability or checkpointing with Options (zeros keep defaults):
//
//	db, err := dorradb.Open("app.dorradb", dorradb.Options{
//	    WALCheckpointBytes: 32 << 20,
//	    DisableSync:        true,
//	})
//
// Files used: path, path+"-wal", path+".lock". Values live on disk; an in-memory
// index maps keys to versioned offsets. Mutations are group-committed to the WAL
// before being applied to the main file and published via a committed sequence.
//
// Same-process MVCC: Get/Has take a statement snapshot at the committed sequence
// and do not wait on the writer goroutine. Multi-process readers are not supported
// yet (exclusive lock on open). A Transaction API can reuse the same pins and
// visibility rules later.
//
// OS I/O goes through an internal VFS (Linux and Windows). File locking uses
// github.com/gofrs/flock. Monotonic sequence numbers on records are the MVCC
// logical timestamps.
package dorradb
