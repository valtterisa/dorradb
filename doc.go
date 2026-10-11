// Package dorradb is an embedded key-value store with a WAL and checkpoints.
//
// Open a database file (created if missing):
//
//	db, err := dorradb.Open("app.dorradb")
//
// Files used: path, path+"-wal", path+".lock". Values live on disk; an in-memory
// index maps keys to offsets. Mutations are group-committed to the WAL before
// being applied to the main file.
//
// Seq numbers on records leave room for future MVCC/OCC without changing Open.
package dorradb
