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
// index maps keys to offsets. Mutations are group-committed to the WAL before
// being applied to the main file.
//
// OS I/O goes through an internal VFS (Linux and Windows). File locking uses
// github.com/gofrs/flock. Seq numbers on records leave room for future MVCC/OCC
// without changing Open.
package dorradb
