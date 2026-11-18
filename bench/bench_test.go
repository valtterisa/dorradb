// Package bench compares DorraDB to SQLite WAL under matched cache/sync.
//
//	go test ./bench -bench=. -benchmem -count=3
//
// Numbers are machine-specific. Peer: modernc.org/sqlite (WAL, mmap_size=0).
package bench_test

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"dorradb"

	_ "modernc.org/sqlite"
)

const (
	keyBytes       = 16
	valSmall       = 64
	valLarge       = 1024
	batchSize      = 100
	fillN          = 100_000
	hotN           = 8_000
	cachePages     = 512
	sqliteCacheKiB = cachePages * 4
)

func keyOf(i int) []byte {
	k := make([]byte, keyBytes)
	binary.BigEndian.PutUint64(k[8:], uint64(i))
	return k
}

func valOf(n, i int) []byte {
	v := make([]byte, n)
	binary.BigEndian.PutUint64(v, uint64(i))
	for j := 8; j < n; j++ {
		v[j] = byte(i + j)
	}
	return v
}

func openDorra(t testing.TB, nosync bool) *dorradb.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bench.dorradb")
	opts := dorradb.Options{CachePages: cachePages}
	if nosync {
		opts.DisableSync = true
	}
	db, err := dorradb.Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func openSQLite(t testing.TB, nosync bool) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bench.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	sync := "FULL"
	if nosync {
		sync = "OFF"
	}
	for _, p := range []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA synchronous=` + sync,
		fmt.Sprintf(`PRAGMA cache_size=%d`, -sqliteCacheKiB),
		`PRAGMA mmap_size=0`,
		`PRAGMA temp_store=MEMORY`,
		`CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB NOT NULL)`,
	} {
		if _, err := db.Exec(p); err != nil {
			_ = db.Close()
			t.Fatalf("%s: %v", p, err)
		}
	}
	return db
}

func dirBytes(dir string) int64 {
	var n int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			n += info.Size()
		}
		return nil
	})
	return n
}

func fillDorra(b *testing.B, db *dorradb.DB, n int) {
	b.Helper()
	v := valOf(valSmall, 0)
	const chunk = 500
	for i := 0; i < n; i += chunk {
		tx, err := db.Begin(true)
		if err != nil {
			b.Fatal(err)
		}
		end := i + chunk
		if end > n {
			end = n
		}
		for j := i; j < end; j++ {
			if _, err := tx.Exec(`INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)`, keyOf(j), v); err != nil {
				_ = tx.Rollback()
				b.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func fillSQLite(b *testing.B, db *sql.DB, n int) {
	b.Helper()
	v := valOf(valSmall, 0)
	const chunk = 500
	for i := 0; i < n; i += chunk {
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		stmt, err := tx.Prepare(`INSERT OR REPLACE INTO kv(k, v) VALUES(?, ?)`)
		if err != nil {
			_ = tx.Rollback()
			b.Fatal(err)
		}
		end := i + chunk
		if end > n {
			end = n
		}
		for j := i; j < end; j++ {
			if _, err := stmt.Exec(keyOf(j), v); err != nil {
				_ = stmt.Close()
				_ = tx.Rollback()
				b.Fatal(err)
			}
		}
		_ = stmt.Close()
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func queryDorra(tx *dorradb.Tx, k []byte) error {
	rows, err := tx.Query(`SELECT v FROM kv WHERE k = ?`, k)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		return dorradb.ErrNotFound
	}
	var got []byte
	return rows.Scan(&got)
}

func BenchmarkPutSeq(b *testing.B) {
	for _, nosync := range []bool{false, true} {
		syncLabel := "sync"
		if nosync {
			syncLabel = "nosync"
		}
		for _, vlen := range []int{valSmall, valLarge} {
			name := fmt.Sprintf("%s/v%d", syncLabel, vlen)
			b.Run("dorradb/"+name, func(b *testing.B) {
				db := openDorra(b, nosync)
				defer db.Close()
				stmt, err := db.Prepare(`INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)`)
				if err != nil {
					b.Fatal(err)
				}
				v := valOf(vlen, 0)
				b.SetBytes(int64(keyBytes + vlen))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := stmt.Exec(keyOf(i), v); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("sqlite/"+name, func(b *testing.B) {
				db := openSQLite(b, nosync)
				defer db.Close()
				v := valOf(vlen, 0)
				stmt, err := db.Prepare(`INSERT OR REPLACE INTO kv(k, v) VALUES(?, ?)`)
				if err != nil {
					b.Fatal(err)
				}
				defer stmt.Close()
				b.SetBytes(int64(keyBytes + vlen))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := stmt.Exec(keyOf(i), v); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkPutBatch(b *testing.B) {
	for _, nosync := range []bool{false, true} {
		syncLabel := "sync"
		if nosync {
			syncLabel = "nosync"
		}
		name := fmt.Sprintf("%s/batch%d/v%d", syncLabel, batchSize, valSmall)
		b.Run("dorradb/"+name, func(b *testing.B) {
			db := openDorra(b, nosync)
			defer db.Close()
			v := valOf(valSmall, 0)
			b.SetBytes(int64(batchSize * (keyBytes + valSmall)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := db.Begin(true)
				if err != nil {
					b.Fatal(err)
				}
				base := i * batchSize
				for j := 0; j < batchSize; j++ {
					if _, err := tx.Exec(`INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)`, keyOf(base+j), v); err != nil {
						_ = tx.Rollback()
						b.Fatal(err)
					}
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("sqlite/"+name, func(b *testing.B) {
			db := openSQLite(b, nosync)
			defer db.Close()
			v := valOf(valSmall, 0)
			b.SetBytes(int64(batchSize * (keyBytes + valSmall)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				base := i * batchSize
				tx, err := db.Begin()
				if err != nil {
					b.Fatal(err)
				}
				stmt, err := tx.Prepare(`INSERT OR REPLACE INTO kv(k, v) VALUES(?, ?)`)
				if err != nil {
					_ = tx.Rollback()
					b.Fatal(err)
				}
				for j := 0; j < batchSize; j++ {
					if _, err := stmt.Exec(keyOf(base+j), v); err != nil {
						_ = stmt.Close()
						_ = tx.Rollback()
						b.Fatal(err)
					}
				}
				_ = stmt.Close()
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGetHot(b *testing.B) {
	name := fmt.Sprintf("fill%d/hot%d/v%d", fillN, hotN, valSmall)
	lo := fillN - hotN
	b.Run("dorradb/"+name, func(b *testing.B) {
		db := openDorra(b, true)
		defer db.Close()
		fillDorra(b, db, fillN)
		rng := rand.New(rand.NewSource(1))
		tx, err := db.Begin(false)
		if err != nil {
			b.Fatal(err)
		}
		defer tx.Rollback()
		b.SetBytes(int64(keyBytes + valSmall))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := queryDorra(tx, keyOf(lo+rng.Intn(hotN))); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("sqlite/"+name, func(b *testing.B) {
		db := openSQLite(b, true)
		defer db.Close()
		fillSQLite(b, db, fillN)
		rng := rand.New(rand.NewSource(1))
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		defer tx.Rollback()
		stmt, err := tx.Prepare(`SELECT v FROM kv WHERE k = ?`)
		if err != nil {
			b.Fatal(err)
		}
		defer stmt.Close()
		b.SetBytes(int64(keyBytes + valSmall))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var got []byte
			if err := stmt.QueryRow(keyOf(lo + rng.Intn(hotN))).Scan(&got); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGetCold(b *testing.B) {
	name := fmt.Sprintf("fill%d/v%d", fillN, valSmall)
	b.Run("dorradb/"+name, func(b *testing.B) {
		db := openDorra(b, true)
		defer db.Close()
		fillDorra(b, db, fillN)
		rng := rand.New(rand.NewSource(1))
		tx, err := db.Begin(false)
		if err != nil {
			b.Fatal(err)
		}
		defer tx.Rollback()
		b.SetBytes(int64(keyBytes + valSmall))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := queryDorra(tx, keyOf(rng.Intn(fillN))); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("sqlite/"+name, func(b *testing.B) {
		db := openSQLite(b, true)
		defer db.Close()
		fillSQLite(b, db, fillN)
		rng := rand.New(rand.NewSource(1))
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		defer tx.Rollback()
		stmt, err := tx.Prepare(`SELECT v FROM kv WHERE k = ?`)
		if err != nil {
			b.Fatal(err)
		}
		defer stmt.Close()
		b.SetBytes(int64(keyBytes + valSmall))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var got []byte
			if err := stmt.QueryRow(keyOf(rng.Intn(fillN))).Scan(&got); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkOverwriteHot(b *testing.B) {
	for _, nosync := range []bool{false, true} {
		syncLabel := "sync"
		if nosync {
			syncLabel = "nosync"
		}
		name := fmt.Sprintf("%s/v%d", syncLabel, valSmall)
		k := keyOf(0)
		b.Run("dorradb/"+name, func(b *testing.B) {
			db := openDorra(b, nosync)
			defer db.Close()
			stmt, err := db.Prepare(`INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)`)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(keyBytes + valSmall))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := stmt.Exec(k, valOf(valSmall, i)); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("sqlite/"+name, func(b *testing.B) {
			db := openSQLite(b, nosync)
			defer db.Close()
			stmt, err := db.Prepare(`INSERT OR REPLACE INTO kv(k, v) VALUES(?, ?)`)
			if err != nil {
				b.Fatal(err)
			}
			defer stmt.Close()
			b.SetBytes(int64(keyBytes + valSmall))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := stmt.Exec(k, valOf(valSmall, i)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDeleteSeq(b *testing.B) {
	for _, nosync := range []bool{false, true} {
		syncLabel := "sync"
		if nosync {
			syncLabel = "nosync"
		}
		name := syncLabel
		b.Run("dorradb/"+name, func(b *testing.B) {
			db := openDorra(b, nosync)
			defer db.Close()
			v := valOf(valSmall, 0)
			ins, err := db.Prepare(`INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)`)
			if err != nil {
				b.Fatal(err)
			}
			del, err := db.Prepare(`DELETE FROM kv WHERE k = ?`)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				if _, err := ins.Exec(keyOf(i), v); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if _, err := del.Exec(keyOf(i)); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("sqlite/"+name, func(b *testing.B) {
			db := openSQLite(b, nosync)
			defer db.Close()
			v := valOf(valSmall, 0)
			ins, err := db.Prepare(`INSERT OR REPLACE INTO kv(k, v) VALUES(?, ?)`)
			if err != nil {
				b.Fatal(err)
			}
			defer ins.Close()
			del, err := db.Prepare(`DELETE FROM kv WHERE k = ?`)
			if err != nil {
				b.Fatal(err)
			}
			defer del.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := keyOf(i)
				b.StopTimer()
				if _, err := ins.Exec(k, v); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if _, err := del.Exec(k); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkFillReport(b *testing.B) {
	b.Run("dorradb", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dir := b.TempDir()
			path := filepath.Join(dir, "bench.dorradb")
			db, err := dorradb.Open(path, dorradb.Options{CachePages: cachePages})
			if err != nil {
				b.Fatal(err)
			}
			if _, err := db.Exec(`CREATE TABLE kv (k BLOB PRIMARY KEY, v BLOB NOT NULL)`); err != nil {
				b.Fatal(err)
			}
			v := valOf(valSmall, 0)
			b.StartTimer()
			tx, err := db.Begin(true)
			if err != nil {
				b.Fatal(err)
			}
			for j := 0; j < fillN; j++ {
				if _, err := tx.Exec(`INSERT INTO kv (k, v) VALUES (?, ?)`, keyOf(j), v); err != nil {
					b.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			b.ReportMetric(float64(dirBytes(dir)), "disk_bytes")
		}
	})
	b.Run("sqlite", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dir := b.TempDir()
			path := filepath.Join(dir, "bench.sqlite")
			db, err := sql.Open("sqlite", path)
			if err != nil {
				b.Fatal(err)
			}
			db.SetMaxOpenConns(1)
			for _, p := range []string{
				`PRAGMA journal_mode=WAL`,
				`PRAGMA synchronous=FULL`,
				fmt.Sprintf(`PRAGMA cache_size=%d`, -sqliteCacheKiB),
				`PRAGMA mmap_size=0`,
				`CREATE TABLE kv (k BLOB PRIMARY KEY, v BLOB NOT NULL)`,
			} {
				if _, err := db.Exec(p); err != nil {
					b.Fatal(err)
				}
			}
			v := valOf(valSmall, 0)
			b.StartTimer()
			tx, err := db.Begin()
			if err != nil {
				b.Fatal(err)
			}
			stmt, err := tx.Prepare(`INSERT INTO kv(k, v) VALUES(?, ?)`)
			if err != nil {
				b.Fatal(err)
			}
			for j := 0; j < fillN; j++ {
				if _, err := stmt.Exec(keyOf(j), v); err != nil {
					b.Fatal(err)
				}
			}
			_ = stmt.Close()
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			b.ReportMetric(float64(dirBytes(dir)), "disk_bytes")
		}
	})
}
