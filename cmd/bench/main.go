// Command bench: DorraDB vs SQLite WAL 1:1 (matched cache + sync).
//
//	go run ./cmd/bench           # writes + reads
//	go run ./cmd/bench writes
//	go run ./cmd/bench reads
//	go run ./cmd/bench help
//
// Peer: modernc.org/sqlite (pure Go), journal_mode=WAL, mmap_size=0,
// cache_size ≈ CachePages×4KiB. Straight timing — no “budget” / pass-fail.
package main

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"dorradb"

	_ "modernc.org/sqlite"
)

const (
	keyLen     = 16
	valLen     = 64
	batchN     = 100
	batches    = 80
	fillN      = 100_000
	hotN       = 8_000
	getIters   = 50_000
	cachePages = 512
	// SQLite negative cache_size is KiB. 512×4 KiB = 2048 KiB.
	sqliteCacheKiB = cachePages * 4
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	cmd := "all"
	if len(args) > 0 {
		cmd = strings.ToLower(args[0])
	}
	switch cmd {
	case "help", "-h", "--help":
		printHelp()
		return 0
	case "all", "reads", "writes", "read", "write":
	default:
		fmt.Fprintf(os.Stderr, "unknown %q\n\n", cmd)
		printHelp()
		return 1
	}
	if cmd == "read" {
		cmd = "reads"
	}
	if cmd == "write" {
		cmd = "writes"
	}

	fmt.Printf("\n  dorradb bench  ·  %s/%s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Printf("  peer: sqlite WAL (modernc)  ·  1:1  ·  lower time wins\n")
	fmt.Printf("  cache: %d pages / %d KiB  ·  fill %d  ·  hot %d  ·  mmap_size=0\n\n",
		cachePages, sqliteCacheKiB, fillN, hotN)

	switch cmd {
	case "all":
		runWrites()
		runReads()
	case "writes":
		runWrites()
	case "reads":
		runReads()
	}

	fmt.Printf("  done.\n\n")
	return 0
}

func printHelp() {
	fmt.Fprintf(os.Stderr, `dorradb bench — 1:1 DorraDB vs SQLite WAL (matched cache/sync)

  go run ./cmd/bench           writes + reads
  go run ./cmd/bench writes    durable + nosync batches
  go run ./cmd/bench reads     hot-set + cold uniform Gets
  go run ./cmd/bench help

Both Go. SQLite: journal_mode=WAL, cache_size=-%d KiB, mmap_size=0,
synchronous=FULL|OFF. No pass/fail budget — who is faster wins.

`, sqliteCacheKiB)
}

func runWrites() {
	section("writes · durable", fmt.Sprintf("%d puts / commit, fsync/FULL", batchN))
	a := must(timePutBatch("dorradb", false))
	b := must(timePutBatch("sqlite", false))
	printPair(a, b, "batch")

	section("writes · nosync", fmt.Sprintf("%d puts / commit, fsync off / OFF", batchN))
	a = must(timePutBatch("dorradb", true))
	b = must(timePutBatch("sqlite", true))
	printPair(a, b, "batch")
}

func runReads() {
	section("reads · hot",
		fmt.Sprintf("fill %d nosync, %d gets over last %d keys, one read tx",
			fillN, getIters, hotN))
	a := must(timeGetHot("dorradb"))
	b := must(timeGetHot("sqlite"))
	printPair(a, b, "get")

	section("reads · cold",
		fmt.Sprintf("same fill, %d uniform gets (DB ≫ %d KiB cache)",
			getIters, sqliteCacheKiB))
	a = must(timeGetCold("dorradb"))
	b = must(timeGetCold("sqlite"))
	printPair(a, b, "get")
}

type result struct {
	name  string
	perOp time.Duration
}

func printPair(a, b result, unit string) {
	fmt.Printf("    %-8s  %10s /%s   %s\n", a.name, fmtDur(a.perOp), unit, fmtRate(a.perOp))
	fmt.Printf("    %-8s  %10s /%s   %s\n", b.name, fmtDur(b.perOp), unit, fmtRate(b.perOp))
	w, l, ratio := winner(a, b)
	fmt.Printf("\n    ★  %s  ·  %.2fx ahead of %s\n\n", w, ratio, l)
}

func section(title, hint string) {
	fmt.Printf("  » %s\n", title)
	fmt.Printf("    %s\n\n", hint)
}

func winner(a, b result) (string, string, float64) {
	if a.perOp <= b.perOp {
		return a.name, b.name, float64(b.perOp) / float64(a.perOp)
	}
	return b.name, a.name, float64(a.perOp) / float64(b.perOp)
}

func must(r result, err error) result {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	return r
}

func timePutBatch(engine string, nosync bool) (result, error) {
	dir, err := os.MkdirTemp("", "bench-*")
	if err != nil {
		return result{}, err
	}
	defer os.RemoveAll(dir)
	v := valOf(0)
	start := time.Now()

	switch engine {
	case "dorradb":
		db, err := dorradb.Open(filepath.Join(dir, "db"), dorradb.Options{
			DisableSync: nosync,
			CachePages:  cachePages,
		})
		if err != nil {
			return result{}, err
		}
		defer db.Close()
		if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB NOT NULL)`); err != nil {
			return result{}, err
		}
		for i := 0; i < batches; i++ {
			tx, err := db.Begin(true)
			if err != nil {
				return result{}, err
			}
			base := i * batchN
			for j := 0; j < batchN; j++ {
				if _, err := tx.Exec(`INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)`, keyOf(base+j), v); err != nil {
					_ = tx.Rollback()
					return result{}, err
				}
			}
			if err := tx.Commit(); err != nil {
				return result{}, err
			}
		}
	case "sqlite":
		db, err := openSQLite(filepath.Join(dir, "db.sqlite"), nosync)
		if err != nil {
			return result{}, err
		}
		defer db.Close()
		for i := 0; i < batches; i++ {
			base := i * batchN
			tx, err := db.Begin()
			if err != nil {
				return result{}, err
			}
			stmt, err := tx.Prepare(`INSERT OR REPLACE INTO kv(k, v) VALUES(?, ?)`)
			if err != nil {
				_ = tx.Rollback()
				return result{}, err
			}
			for j := 0; j < batchN; j++ {
				if _, err := stmt.Exec(keyOf(base+j), v); err != nil {
					_ = stmt.Close()
					_ = tx.Rollback()
					return result{}, err
				}
			}
			_ = stmt.Close()
			if err := tx.Commit(); err != nil {
				return result{}, err
			}
		}
	}
	elapsed := time.Since(start)
	return result{engine, elapsed / time.Duration(batches)}, nil
}

func timeGetHot(engine string) (result, error) {
	return timeGetRange(engine, fillN-hotN, fillN)
}

func timeGetCold(engine string) (result, error) {
	return timeGetRange(engine, 0, fillN)
}

func timeGetRange(engine string, lo, hi int) (result, error) {
	if hi <= lo {
		return result{}, fmt.Errorf("empty get range")
	}
	dir, err := os.MkdirTemp("", "bench-*")
	if err != nil {
		return result{}, err
	}
	defer os.RemoveAll(dir)
	v := valOf(0)
	rng := rand.New(rand.NewSource(1))
	span := hi - lo
	var elapsed time.Duration

	switch engine {
	case "dorradb":
		db, err := dorradb.Open(filepath.Join(dir, "db"), dorradb.Options{
			DisableSync: true,
			CachePages:  cachePages,
		})
		if err != nil {
			return result{}, err
		}
		defer db.Close()
		if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB NOT NULL)`); err != nil {
			return result{}, err
		}
		if err := fillDorra(db, v); err != nil {
			return result{}, err
		}
		tx, err := db.Begin(false)
		if err != nil {
			return result{}, err
		}
		start := time.Now()
		for i := 0; i < getIters; i++ {
			rows, err := tx.Query(`SELECT v FROM kv WHERE k = ?`, keyOf(lo+rng.Intn(span)))
			if err != nil {
				_ = tx.Rollback()
				return result{}, err
			}
			if !rows.Next() {
				_ = rows.Close()
				_ = tx.Rollback()
				return result{}, dorradb.ErrNotFound
			}
			var got []byte
			if err := rows.Scan(&got); err != nil {
				_ = rows.Close()
				_ = tx.Rollback()
				return result{}, err
			}
			_ = rows.Close()
		}
		elapsed = time.Since(start)
		_ = tx.Rollback()
	case "sqlite":
		db, err := openSQLite(filepath.Join(dir, "db.sqlite"), true)
		if err != nil {
			return result{}, err
		}
		defer db.Close()
		if err := fillSQLite(db, v); err != nil {
			return result{}, err
		}
		tx, err := db.Begin()
		if err != nil {
			return result{}, err
		}
		stmt, err := tx.Prepare(`SELECT v FROM kv WHERE k = ?`)
		if err != nil {
			_ = tx.Rollback()
			return result{}, err
		}
		start := time.Now()
		for i := 0; i < getIters; i++ {
			var got []byte
			if err := stmt.QueryRow(keyOf(lo + rng.Intn(span))).Scan(&got); err != nil {
				_ = stmt.Close()
				_ = tx.Rollback()
				return result{}, err
			}
		}
		elapsed = time.Since(start)
		_ = stmt.Close()
		_ = tx.Rollback()
	}
	return result{engine, elapsed / time.Duration(getIters)}, nil
}

func openSQLite(path string, nosync bool) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	sync := "FULL"
	if nosync {
		sync = "OFF"
	}
	pragmas := []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA synchronous=` + sync,
		fmt.Sprintf(`PRAGMA cache_size=%d`, -sqliteCacheKiB),
		`PRAGMA mmap_size=0`,
		`PRAGMA temp_store=MEMORY`,
		`CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB NOT NULL)`,
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("%s: %w", p, err)
		}
	}
	return db, nil
}

func fillDorra(db *dorradb.DB, v []byte) error {
	const chunk = 500
	for i := 0; i < fillN; i += chunk {
		tx, err := db.Begin(true)
		if err != nil {
			return err
		}
		end := i + chunk
		if end > fillN {
			end = fillN
		}
		for j := i; j < end; j++ {
			if _, err := tx.Exec(`INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)`, keyOf(j), v); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func fillSQLite(db *sql.DB, v []byte) error {
	const chunk = 500
	for i := 0; i < fillN; i += chunk {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		stmt, err := tx.Prepare(`INSERT OR REPLACE INTO kv(k, v) VALUES(?, ?)`)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		end := i + chunk
		if end > fillN {
			end = fillN
		}
		for j := i; j < end; j++ {
			if _, err := stmt.Exec(keyOf(j), v); err != nil {
				_ = stmt.Close()
				_ = tx.Rollback()
				return err
			}
		}
		_ = stmt.Close()
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func fmtDur(d time.Duration) string {
	switch {
	case d >= time.Millisecond:
		return fmt.Sprintf("%.2f ms", float64(d)/float64(time.Millisecond))
	case d >= time.Microsecond:
		return fmt.Sprintf("%.2f µs", float64(d)/float64(time.Microsecond))
	default:
		return fmt.Sprintf("%d ns", d.Nanoseconds())
	}
}

func fmtRate(perOp time.Duration) string {
	if perOp <= 0 {
		return ""
	}
	ops := float64(time.Second) / float64(perOp)
	switch {
	case ops >= 1e6:
		return fmt.Sprintf("%.2f M/s", ops/1e6)
	case ops >= 1e3:
		return fmt.Sprintf("%.1f k/s", ops/1e3)
	default:
		return fmt.Sprintf("%.0f /s", ops)
	}
}

func keyOf(i int) []byte {
	k := make([]byte, keyLen)
	binary.BigEndian.PutUint64(k[8:], uint64(i))
	return k
}

func valOf(i int) []byte {
	v := make([]byte, valLen)
	binary.BigEndian.PutUint64(v, uint64(i))
	for j := 8; j < valLen; j++ {
		v[j] = byte(i + j)
	}
	return v
}
