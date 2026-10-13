// Package dorradb is an embedded key-value store with a WAL, 4 KiB pager, and checkpoints.
//
// DorraDB provides ACID transactions. Every read and write runs inside a
// transaction (AUTOCOMMIT for single Get/Set/Delete/Has, or explicit
// Begin / Commit / Rollback). Commits are atomic and durable (WAL sync on by
// default); readers use snapshot isolation; one writer at a time.
//
//	db, err := dorradb.Open("app.dorradb")
//
//	tx, err := db.Begin(true)
//	defer tx.Rollback()
//	_ = tx.Set([]byte("a"), []byte("1"))
//	_ = tx.Set([]byte("b"), []byte("2"))
//	err = tx.Commit()
//
// Files: path, path+"-wal", path+".lock". One process opens a path (exclusive
// lock). Do not hold transactions open longer than needed.
package dorradb
