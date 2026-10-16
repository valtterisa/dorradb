package api_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"dorradb"
	"dorradb/internal/engine"
	"dorradb/tests/harness"
)

func TestSetGetReopen(t *testing.T) {
	db, path := harness.Open(t, dorradb.Options{})
	if err := db.Set([]byte("name"), []byte("Ada")); err != nil {
		t.Fatal(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "Ada" {
		t.Fatalf("got %q", val)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := dorradb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	val, err = db2.Get([]byte("name"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "Ada" {
		t.Fatalf("reopen got %q", val)
	}
}

func TestDelete(t *testing.T) {
	db, _ := harness.Open(t, dorradb.Options{})
	defer db.Close()
	_ = db.Set([]byte("a"), []byte("1"))
	if err := db.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	_, err := db.Get([]byte("a"))
	if !errors.Is(err, dorradb.ErrNotFound) {
		t.Fatalf("err=%v", err)
	}
}

func TestMainFileIsPaged(t *testing.T) {
	db, path := harness.Open(t, dorradb.Options{})
	_ = db.Set([]byte("k"), []byte("v"))
	_ = db.Close()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size()%4096 != 0 {
		t.Fatalf("size %d not multiple of 4096", info.Size())
	}
}

func TestCachePagesOption(t *testing.T) {
	db, _ := harness.Open(t, dorradb.Options{CachePages: 8, DisableSync: true})
	defer db.Close()
	if err := db.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if db.Stats().CachePages != 8 {
		t.Fatalf("got %d", db.Stats().CachePages)
	}
}

func TestErrLocked(t *testing.T) {
	db, path := harness.Open(t, dorradb.Options{})
	defer db.Close()
	_, err := dorradb.Open(path)
	if !errors.Is(err, dorradb.ErrLocked) {
		t.Fatalf("err=%v want ErrLocked", err)
	}
}

func TestCloseReturnsFlushError(t *testing.T) {
	fs := harness.NewFaultFS(nil)
	eng, _ := harness.OpenEngine(t, fs, engine.Options{SyncWAL: false, CachePages: 4})
	if err := eng.Commit([]engine.Mutation{{Key: []byte("k"), Value: []byte("v")}}); err != nil {
		t.Fatal(err)
	}
	// Fail the next Sync (Close: WAL Sync or main FlushAll Sync).
	fs.FailSyncAfter = fs.SyncCount()
	err := eng.Close()
	if err == nil {
		t.Fatal("Close should return error when Sync fails")
	}
}

func TestKeyLimits(t *testing.T) {
	db, _ := harness.Open(t, dorradb.Options{DisableSync: true})
	defer db.Close()
	if err := db.Set(nil, []byte("v")); !errors.Is(err, dorradb.ErrInvalidKey) {
		t.Fatalf("empty key: %v", err)
	}
	big := make([]byte, 1025)
	if err := db.Set(big, []byte("v")); !errors.Is(err, dorradb.ErrTooLarge) {
		t.Fatalf("big key: %v", err)
	}
}

func TestVacuumAfterCheckpoint(t *testing.T) {
	db, _ := harness.Open(t, dorradb.Options{
		WALCheckpointBytes: 1,
		VacuumDeadBytes:    1,
		DisableSync:        true,
	})
	defer db.Close()

	val := make([]byte, 500)
	for i := 0; i < 30; i++ {
		if err := db.Set([]byte{byte(i)}, val); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 30; i++ {
		if err := db.Set([]byte{byte(i)}, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Set([]byte("z"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		got, err := db.Get([]byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "x" {
			t.Fatalf("key %d = %q", i, got)
		}
	}
}

func TestDeprecatedOptionsStillOpen(t *testing.T) {
	db, err := dorradb.Open(filepath.Join(t.TempDir(), "db"), dorradb.Options{
		GroupCommitMaxOps:  64,
		GroupCommitMaxWait: 0,
		DisableSync:        true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
}
