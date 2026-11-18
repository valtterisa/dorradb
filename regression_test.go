package dorradb_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"dorradb"
)

func TestRegressionHotKeyUpdates(t *testing.T) {
	db, _ := dorradb.Open(filepath.Join(t.TempDir(), "a.dorradb"), dorradb.Options{DisableSync: true})
	defer db.Close()
	_, _ = db.Exec(`CREATE TABLE t (k TEXT PRIMARY KEY, v BLOB NOT NULL)`)
	v := bytes.Repeat([]byte("x"), 1000)
	for i := 0; i < 10; i++ {
		if _, err := db.Exec(`INSERT OR REPLACE INTO t (k, v) VALUES (?, ?)`, "k", v); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}
}

func TestRegressionBigInlineSplit(t *testing.T) {
	db, _ := dorradb.Open(filepath.Join(t.TempDir(), "a.dorradb"), dorradb.Options{DisableSync: true})
	defer db.Close()
	_, _ = db.Exec(`CREATE TABLE t (k BLOB PRIMARY KEY, v BLOB NOT NULL)`)
	v := bytes.Repeat([]byte("y"), 1900)
	for i := 0; i < 6; i++ {
		if _, err := db.Exec(`INSERT INTO t (k, v) VALUES (?, ?)`, []byte{byte('a' + i)}, v); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	for i := 0; i < 6; i++ {
		rows, err := db.Query(`SELECT v FROM t WHERE k = ?`, []byte{byte('a' + i)})
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !rows.Next() {
			t.Fatalf("missing %d", i)
		}
		_ = rows.Close()
	}
}

func TestRegressionConcurrentReadWrite(t *testing.T) {
	db, _ := dorradb.Open(filepath.Join(t.TempDir(), "a.dorradb"), dorradb.Options{DisableSync: true, CachePages: 8})
	defer db.Close()
	_, _ = db.Exec(`CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT NOT NULL)`)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := 0; i < 50; i++ {
					rows, _ := db.Query(`SELECT v FROM t WHERE k = ?`, fmt.Sprintf("k%04d", i))
					if rows != nil {
						_ = rows.Close()
					}
				}
			}
		}()
	}
	for i := 0; i < 2000; i++ {
		if _, err := db.Exec(`INSERT OR REPLACE INTO t (k, v) VALUES (?, ?)`,
			fmt.Sprintf("k%04d", i%500), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}

func TestRegressionCrashWithSmallCache(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.dorradb")
	db, _ := dorradb.Open(path, dorradb.Options{DisableSync: true, CachePages: 8})
	_, _ = db.Exec(`CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT NOT NULL)`)
	const n = 2000
	for i := 0; i < n; i++ {
		if _, err := db.Exec(`INSERT INTO t (k, v) VALUES (?, ?)`,
			fmt.Sprintf("k%05d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	crash := filepath.Join(t.TempDir(), "c.dorradb")
	for _, suf := range []string{"", "-wal"} {
		b, err := os.ReadFile(path + suf)
		if err != nil {
			t.Fatal(err)
		}
		_ = os.WriteFile(crash+suf, b, 0o644)
	}
	db.Close()
	db2, err := dorradb.Open(crash, dorradb.Options{DisableSync: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	for i := 0; i < n; i++ {
		rows, err := db2.Query(`SELECT v FROM t WHERE k = ?`, fmt.Sprintf("k%05d", i))
		if err != nil {
			t.Fatalf("i=%d err=%v", i, err)
		}
		if !rows.Next() {
			t.Fatalf("i=%d missing", i)
		}
		var got string
		if err := rows.Scan(&got); err != nil || got != fmt.Sprintf("v%d", i) {
			t.Fatalf("i=%d got %q err=%v", i, got, err)
		}
		_ = rows.Close()
	}
}
