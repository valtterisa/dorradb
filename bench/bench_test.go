// Package bench compares DorraDB to peer embedded stores under the same workloads.
//
//	go test ./bench -bench=. -benchmem -count=3
//	go test ./bench -bench=Put -benchmem          # subset
//	go test ./bench -bench=. -benchtime=3s
//
// Numbers are machine-specific. Re-run on the same disk/OS when comparing changes.
package bench_test

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"dorradb"

	bolt "go.etcd.io/bbolt"
)

const (
	bucketName = "kv"
	keyBytes   = 16
	valSmall   = 64
	valLarge   = 1024
	batchSize  = 100
	fillN      = 10_000
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
	opts := dorradb.Options{}
	if nosync {
		opts.DisableSync = true
	}
	db, err := dorradb.Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func openBolt(t testing.TB, nosync bool) *bolt.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bench.bolt")
	db, err := bolt.Open(path, 0o600, &bolt.Options{NoSync: nosync})
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
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

// --- sequential puts ---

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
				v := valOf(vlen, 0)
				b.SetBytes(int64(keyBytes + vlen))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := db.Set(keyOf(i), v); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("bbolt/"+name, func(b *testing.B) {
				db := openBolt(b, nosync)
				defer db.Close()
				v := valOf(vlen, 0)
				b.SetBytes(int64(keyBytes + vlen))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					err := db.Update(func(tx *bolt.Tx) error {
						return tx.Bucket([]byte(bucketName)).Put(keyOf(i), v)
					})
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// --- batched puts (many keys per commit) ---

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
					if err := tx.Set(keyOf(base+j), v); err != nil {
						_ = tx.Rollback()
						b.Fatal(err)
					}
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("bbolt/"+name, func(b *testing.B) {
			db := openBolt(b, nosync)
			defer db.Close()
			v := valOf(valSmall, 0)
			b.SetBytes(int64(batchSize * (keyBytes + valSmall)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				base := i * batchSize
				err := db.Update(func(tx *bolt.Tx) error {
					bk := tx.Bucket([]byte(bucketName))
					for j := 0; j < batchSize; j++ {
						if err := bk.Put(keyOf(base+j), v); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// --- fills then random gets ---

func BenchmarkGetRand(b *testing.B) {
	for _, nosync := range []bool{false, true} {
		syncLabel := "sync"
		if nosync {
			syncLabel = "nosync"
		}
		name := fmt.Sprintf("%s/fill%d/v%d", syncLabel, fillN, valSmall)
		b.Run("dorradb/"+name, func(b *testing.B) {
			db := openDorra(b, nosync)
			defer db.Close()
			v := valOf(valSmall, 0)
			for i := 0; i < fillN; i++ {
				if err := db.Set(keyOf(i), v); err != nil {
					b.Fatal(err)
				}
			}
			rng := rand.New(rand.NewSource(1))
			b.SetBytes(int64(keyBytes + valSmall))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := keyOf(rng.Intn(fillN))
				if _, err := db.Get(k); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("bbolt/"+name, func(b *testing.B) {
			db := openBolt(b, nosync)
			defer db.Close()
			v := valOf(valSmall, 0)
			err := db.Update(func(tx *bolt.Tx) error {
				bk := tx.Bucket([]byte(bucketName))
				for i := 0; i < fillN; i++ {
					if err := bk.Put(keyOf(i), v); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			rng := rand.New(rand.NewSource(1))
			b.SetBytes(int64(keyBytes + valSmall))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := keyOf(rng.Intn(fillN))
				err := db.View(func(tx *bolt.Tx) error {
					if tx.Bucket([]byte(bucketName)).Get(k) == nil {
						return fmt.Errorf("missing")
					}
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// --- overwrite hot key (MVCC / page rewrite pressure) ---

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
			b.SetBytes(int64(keyBytes + valSmall))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.Set(k, valOf(valSmall, i)); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("bbolt/"+name, func(b *testing.B) {
			db := openBolt(b, nosync)
			defer db.Close()
			b.SetBytes(int64(keyBytes + valSmall))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v := valOf(valSmall, i)
				err := db.Update(func(tx *bolt.Tx) error {
					return tx.Bucket([]byte(bucketName)).Put(k, v)
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// --- delete after fill ---

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
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				if err := db.Set(keyOf(i), v); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if err := db.Delete(keyOf(i)); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("bbolt/"+name, func(b *testing.B) {
			db := openBolt(b, nosync)
			defer db.Close()
			v := valOf(valSmall, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := keyOf(i)
				b.StopTimer()
				err := db.Update(func(tx *bolt.Tx) error {
					return tx.Bucket([]byte(bucketName)).Put(k, v)
				})
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				err = db.Update(func(tx *bolt.Tx) error {
					return tx.Bucket([]byte(bucketName)).Delete(k)
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFillReport writes fillN keys once and reports wall time + on-disk size.
// Useful as a one-shot sizing check: go test ./bench -bench=FillReport -benchtime=1x
func BenchmarkFillReport(b *testing.B) {
	b.Run("dorradb", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dir := b.TempDir()
			path := filepath.Join(dir, "bench.dorradb")
			db, err := dorradb.Open(path)
			if err != nil {
				b.Fatal(err)
			}
			v := valOf(valSmall, 0)
			b.StartTimer()
			for j := 0; j < fillN; j++ {
				if err := db.Set(keyOf(j), v); err != nil {
					b.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			b.ReportMetric(float64(dirBytes(dir)), "disk_bytes")
		}
	})
	b.Run("bbolt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dir := b.TempDir()
			path := filepath.Join(dir, "bench.bolt")
			db, err := bolt.Open(path, 0o600, nil)
			if err != nil {
				b.Fatal(err)
			}
			v := valOf(valSmall, 0)
			b.StartTimer()
			err = db.Update(func(tx *bolt.Tx) error {
				bk, err := tx.CreateBucketIfNotExists([]byte(bucketName))
				if err != nil {
					return err
				}
				for j := 0; j < fillN; j++ {
					if err := bk.Put(keyOf(j), v); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
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
