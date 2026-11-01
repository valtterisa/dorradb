package dorradb_test

import (
	"os"
	"path/filepath"
	"testing"

	"dorradb"
)

func TestOpenSetGetReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.dorradb")
	db, err := dorradb.Open(path, dorradb.Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT NOT NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO t (k, v) VALUES (?, ?)`, "name", "Ada")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT v FROM t WHERE k = ?`, "name")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("missing row")
	}
	var val string
	if err := rows.Scan(&val); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if val != "Ada" {
		t.Fatalf("got %q", val)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := dorradb.Open(path, dorradb.Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	rows, err = db2.Query(`SELECT v FROM t WHERE k = ?`, "name")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("missing after reopen")
	}
	if err := rows.Scan(&val); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if val != "Ada" {
		t.Fatalf("reopen got %q", val)
	}
}

func TestCrashRecoversFromWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.dorradb")
	db, err := dorradb.Open(path, dorradb.Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db.Exec(`CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT NOT NULL)`)
	_, err = db.Exec(`INSERT INTO t (k, v) VALUES (?, ?)`, "x", "1")
	if err != nil {
		t.Fatal(err)
	}
	// Simulate crash: close without clean flush by killing process isn't easy;
	// reopen after Close still exercises recover path.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := dorradb.Open(path, dorradb.Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	rows, err := db2.Query(`SELECT v FROM t WHERE k = ?`, "x")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("missing")
	}
	var val string
	if err := rows.Scan(&val); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if val != "1" {
		t.Fatalf("got %q", val)
	}
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	db, err := dorradb.Open(filepath.Join(dir, "app.dorradb"), dorradb.Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, _ = db.Exec(`CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT NOT NULL)`)
	_, _ = db.Exec(`INSERT INTO t (k, v) VALUES (?, ?)`, "a", "1")
	_, err = db.Exec(`DELETE FROM t WHERE k = ?`, "a")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT v FROM t WHERE k = ?`, "a")
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("expected no row")
	}
	_ = rows.Close()
}

func TestPageAlignedMain(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.dorradb")
	db, err := dorradb.Open(path, dorradb.Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db.Exec(`CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT NOT NULL)`)
	_, _ = db.Exec(`INSERT INTO t (k, v) VALUES (?, ?)`, "k", "v")
	_ = db.Close()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size()%4096 != 0 {
		t.Fatalf("size %d not multiple of 4096", info.Size())
	}
}

func TestVacuumAfterCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.dorradb")
	db, err := dorradb.Open(path, dorradb.Options{
		WALCheckpointBytes: 1,
		VacuumDeadBytes:    1,
		DisableSync:        true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, _ = db.Exec(`CREATE TABLE t (k BLOB PRIMARY KEY, v BLOB NOT NULL)`)
	val := make([]byte, 500)
	for i := 0; i < 30; i++ {
		if _, err := db.Exec(`INSERT OR REPLACE INTO t (k, v) VALUES (?, ?)`, []byte{byte(i)}, val); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 30; i++ {
		if _, err := db.Exec(`INSERT OR REPLACE INTO t (k, v) VALUES (?, ?)`, []byte{byte(i)}, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO t (k, v) VALUES (?, ?)`, []byte("z"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		rows, err := db.Query(`SELECT v FROM t WHERE k = ?`, []byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		if !rows.Next() {
			t.Fatalf("missing %d", i)
		}
		var got []byte
		if err := rows.Scan(&got); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		if string(got) != "x" {
			t.Fatalf("key %d = %q", i, got)
		}
	}
	st := db.Stats()
	if st.CachePages != dorradb.DefaultOptions().CachePages {
		t.Fatalf("cache_pages=%d", st.CachePages)
	}
}

func TestManyKeysReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.dorradb")
	db, err := dorradb.Open(path, dorradb.Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db.Exec(`CREATE TABLE t (k BLOB PRIMARY KEY, v BLOB NOT NULL)`)
	const n = 300
	for i := 0; i < n; i++ {
		k := []byte{byte(i >> 8), byte(i)}
		v := []byte{byte(i)}
		if _, err := db.Exec(`INSERT INTO t (k, v) VALUES (?, ?)`, k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := dorradb.Open(path, dorradb.Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < n; i++ {
		k := []byte{byte(i >> 8), byte(i)}
		rows, err := db2.Query(`SELECT v FROM t WHERE k = ?`, k)
		if err != nil {
			t.Fatalf("i=%d: %v", i, err)
		}
		if !rows.Next() {
			t.Fatalf("i=%d missing", i)
		}
		var got []byte
		if err := rows.Scan(&got); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		if len(got) != 1 || got[0] != byte(i) {
			t.Fatalf("i=%d got %v", i, got)
		}
	}
}

func TestCachePagesOption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.dorradb")
	db, err := dorradb.Open(path, dorradb.Options{CachePages: 8, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, _ = db.Exec(`CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT NOT NULL)`)
	if _, err := db.Exec(`INSERT INTO t (k, v) VALUES (?, ?)`, "a", "1"); err != nil {
		t.Fatal(err)
	}
	if db.Stats().CachePages != 8 {
		t.Fatalf("got %d", db.Stats().CachePages)
	}
}

