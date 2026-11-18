// Package dorradb is an embedded relational store for Go.
//
// Open a file, run SQL (CREATE / INSERT / UPDATE / DELETE / SELECT), optionally
// wrap work in an explicit transaction. Under the hood: clustered B+ trees on
// 4 KiB slotted pages, a write-ahead log, and snapshot MVCC.
//
//	db, err := dorradb.Open("app.dorradb")
//	_, err = db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
//	_, err = db.Exec(`INSERT INTO t (id, name) VALUES (?, ?)`, int64(1), "Ada")
//	rows, err := db.Query(`SELECT name FROM t WHERE id = ?`, int64(1))
//
// Or use the database/sql driver registered as "dorradb".
package dorradb
