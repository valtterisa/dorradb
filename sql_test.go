package dorradb_test

import (
	"path/filepath"
	"testing"

	"dorradb"
)

func TestSQLPhase1to3(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.dorradb")

	db, err := dorradb.Open(path, dorradb.Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB NOT NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO kv (k, v) VALUES (?, ?)`, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)`, []byte("a"), []byte("2"))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT v FROM kv WHERE k = ?`, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("expected row")
	}
	var v []byte
	if err := rows.Scan(&v); err != nil {
		t.Fatal(err)
	}
	if string(v) != "2" {
		t.Fatalf("got %q", v)
	}
	_ = rows.Close()

	_, err = db.Exec(`UPDATE kv SET v = ? WHERE k = ?`, []byte("3"), []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`DELETE FROM kv WHERE k = ?`, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}

	// Phase 2: typed multi-column
	_, err = db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, data BLOB)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO users (id, name, data) VALUES (?, ?, ?)`, int64(1), "alice", []byte{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO users (id, name, data) VALUES (2, 'bob', X'ff00')`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err = db.Query(`SELECT id, name FROM users WHERE id = ?`, int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("expected user")
	}
	var id int64
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatal(err)
	}
	if id != 1 || name != "alice" {
		t.Fatalf("got %d %q", id, name)
	}
	_ = rows.Close()

	rows, err = db.Query(`SELECT * FROM users`)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	_ = rows.Close()
	if n != 2 {
		t.Fatalf("scan got %d rows", n)
	}

	// Phase 3: second table + reopen
	_, err = db.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, title TEXT NOT NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO items (id, title) VALUES (10, 'x')`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := dorradb.Open(path, dorradb.Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	rows, err = db2.Query(`SELECT name FROM users WHERE id = 2`)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("expected bob after reopen")
	}
	if err := rows.Scan(&name); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if name != "bob" {
		t.Fatalf("got %q", name)
	}

	rows, err = db2.Query(`SELECT title FROM items WHERE id = 10`)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("expected item after reopen")
	}
	var title string
	if err := rows.Scan(&title); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if title != "x" {
		t.Fatalf("got %q", title)
	}
}

func TestSQLParserReject(t *testing.T) {
	dir := t.TempDir()
	db, err := dorradb.Open(filepath.Join(dir, "t.dorradb"), dorradb.Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`SELECT * FROM t JOIN u`)
	if err == nil {
		t.Fatal("expected parse error")
	}
}
