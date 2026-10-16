package tx_test

import (
	"errors"
	"testing"

	"dorradb"
	"dorradb/tests/harness"
)

func TestSnapshotIsolation(t *testing.T) {
	db, _ := harness.Open(t, dorradb.Options{DisableSync: true})
	defer db.Close()
	if err := db.Set([]byte("k"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	if err := db.Set([]byte("k"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	got, err := tx.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old" {
		t.Fatalf("snapshot saw %q want old", got)
	}
	got, err = db.Get([]byte("k"))
	if err != nil || string(got) != "new" {
		t.Fatalf("db got %q %v", got, err)
	}
}

func TestReadYourWrites(t *testing.T) {
	db, _ := harness.Open(t, dorradb.Options{DisableSync: true})
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := tx.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	got, err := tx.Get([]byte("a"))
	if err != nil || string(got) != "1" {
		t.Fatalf("ryw get: %q %v", got, err)
	}
	if err := tx.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Get([]byte("a"))
	if !errors.Is(err, dorradb.ErrNotFound) {
		t.Fatalf("after delete: %v", err)
	}
}

func TestMultiKeyCommitRollback(t *testing.T) {
	db, _ := harness.Open(t, dorradb.Options{DisableSync: true})
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Set([]byte("a"), []byte("1"))
	_ = tx.Set([]byte("b"), []byte("2"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"a", "b"} {
		if _, err := db.Get([]byte(k)); err != nil {
			t.Fatal(err)
		}
	}

	tx2, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	_ = tx2.Set([]byte("c"), []byte("3"))
	_ = tx2.Set([]byte("d"), []byte("4"))
	if err := tx2.Rollback(); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"c", "d"} {
		_, err := db.Get([]byte(k))
		if !errors.Is(err, dorradb.ErrNotFound) {
			t.Fatalf("%s should be absent", k)
		}
	}
}

func TestTxErrors(t *testing.T) {
	db, _ := harness.Open(t, dorradb.Options{DisableSync: true})
	defer db.Close()

	rtx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := rtx.Set([]byte("a"), []byte("1")); !errors.Is(err, dorradb.ErrTxNotWritable) {
		t.Fatalf("set on ro: %v", err)
	}
	_ = rtx.Rollback()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Commit()
	if err := tx.Set([]byte("a"), []byte("1")); !errors.Is(err, dorradb.ErrTxClosed) {
		t.Fatalf("set after commit: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, dorradb.ErrTxClosed) {
		t.Fatalf("double commit: %v", err)
	}
}

func TestEmptyCommit(t *testing.T) {
	db, _ := harness.Open(t, dorradb.Options{DisableSync: true})
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if db.Stats().PinnedReaders != 0 {
		t.Fatalf("pins=%d", db.Stats().PinnedReaders)
	}
}

func TestPinnedReadersInStats(t *testing.T) {
	db, _ := harness.Open(t, dorradb.Options{DisableSync: true})
	defer db.Close()
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if db.Stats().PinnedReaders < 1 {
		t.Fatalf("expected pinned reader")
	}
	_ = tx.Rollback()
	if db.Stats().PinnedReaders != 0 {
		t.Fatalf("pins after rollback=%d", db.Stats().PinnedReaders)
	}
}
