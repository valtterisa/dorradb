// Command bench: quick DorraDB vs bbolt timing in the terminal.
//
//	go run ./cmd/bench           # everything
//	go run ./cmd/bench reads     # random gets
//	go run ./cmd/bench writes    # batched puts (sync + nosync)
//	go run ./cmd/bench help
package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"dorradb"

	bolt "go.etcd.io/bbolt"
)

const (
	bucket   = "kv"
	keyLen   = 16
	valLen   = 64
	batchN   = 100
	fillN    = 10_000
	batches  = 80
	getIters = 40_000
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
	fmt.Printf("  vs bbolt  ·  lower time wins\n")
	fmt.Printf("  %s\n\n", strings.Repeat("─", 40))

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
	fmt.Fprintf(os.Stderr, `dorradb bench — tiny local speed check vs bbolt

  go run ./cmd/bench           all (writes + reads)
  go run ./cmd/bench writes    batched puts (durable + nosync)
  go run ./cmd/bench reads     fill 10k then random gets
  go run ./cmd/bench help

`)
}

func runWrites() {
	section("writes · durable", fmt.Sprintf("%d puts / commit, fsync on", batchN))
	a := must(timePutBatch("dorradb", false))
	b := must(timePutBatch("bbolt", false))
	printPair(a, b, "batch")

	section("writes · nosync", fmt.Sprintf("%d puts / commit, fsync off", batchN))
	a = must(timePutBatch("dorradb", true))
	b = must(timePutBatch("bbolt", true))
	printPair(a, b, "batch")
}

func runReads() {
	section("reads · random", fmt.Sprintf("fill %d, then %d gets", fillN, getIters))
	a := must(timeGetRand("dorradb"))
	b := must(timeGetRand("bbolt"))
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
		db, err := dorradb.Open(filepath.Join(dir, "db"), dorradb.Options{DisableSync: nosync})
		if err != nil {
			return result{}, err
		}
		defer db.Close()
		for i := 0; i < batches; i++ {
			tx, err := db.Begin(true)
			if err != nil {
				return result{}, err
			}
			base := i * batchN
			for j := 0; j < batchN; j++ {
				if err := tx.Set(keyOf(base+j), v); err != nil {
					_ = tx.Rollback()
					return result{}, err
				}
			}
			if err := tx.Commit(); err != nil {
				return result{}, err
			}
		}
	case "bbolt":
		db, err := bolt.Open(filepath.Join(dir, "db"), 0o600, &bolt.Options{NoSync: nosync})
		if err != nil {
			return result{}, err
		}
		defer db.Close()
		if err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(bucket))
			return err
		}); err != nil {
			return result{}, err
		}
		for i := 0; i < batches; i++ {
			base := i * batchN
			err := db.Update(func(tx *bolt.Tx) error {
				bk := tx.Bucket([]byte(bucket))
				for j := 0; j < batchN; j++ {
					if err := bk.Put(keyOf(base+j), v); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return result{}, err
			}
		}
	}
	elapsed := time.Since(start)
	return result{engine, elapsed / time.Duration(batches)}, nil
}

func timeGetRand(engine string) (result, error) {
	dir, err := os.MkdirTemp("", "bench-*")
	if err != nil {
		return result{}, err
	}
	defer os.RemoveAll(dir)
	v := valOf(0)
	rng := rand.New(rand.NewSource(1))
	var elapsed time.Duration

	switch engine {
	case "dorradb":
		db, err := dorradb.Open(filepath.Join(dir, "db"))
		if err != nil {
			return result{}, err
		}
		defer db.Close()
		for i := 0; i < fillN; i++ {
			if err := db.Set(keyOf(i), v); err != nil {
				return result{}, err
			}
		}
		start := time.Now()
		for i := 0; i < getIters; i++ {
			if _, err := db.Get(keyOf(rng.Intn(fillN))); err != nil {
				return result{}, err
			}
		}
		elapsed = time.Since(start)
	case "bbolt":
		db, err := bolt.Open(filepath.Join(dir, "db"), 0o600, nil)
		if err != nil {
			return result{}, err
		}
		defer db.Close()
		if err := db.Update(func(tx *bolt.Tx) error {
			bk, err := tx.CreateBucketIfNotExists([]byte(bucket))
			if err != nil {
				return err
			}
			for i := 0; i < fillN; i++ {
				if err := bk.Put(keyOf(i), v); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return result{}, err
		}
		start := time.Now()
		for i := 0; i < getIters; i++ {
			k := keyOf(rng.Intn(fillN))
			err := db.View(func(tx *bolt.Tx) error {
				if tx.Bucket([]byte(bucket)).Get(k) == nil {
					return fmt.Errorf("missing")
				}
				return nil
			})
			if err != nil {
				return result{}, err
			}
		}
		elapsed = time.Since(start)
	}
	return result{engine, elapsed / time.Duration(getIters)}, nil
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
