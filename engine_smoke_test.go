package dorradb_test

import (
	"os"
	"path/filepath"
	"testing"

	"dorradb"
	"dorradb/internal/encode"
	"dorradb/internal/storage/page"
)

func TestSetGetReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.dorradb")
	db, err := dorradb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
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

func TestRecoveryFromWALWhenMainLags(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.dorradb")
	db, err := dorradb.Open(path, dorradb.Options{
		WALCheckpointBytes: 1 << 30, // avoid truncating WAL
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("x"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate crash after WAL durable but before pages persisted:
	// reset main to meta-only with sequence 0; keep WAL intact.
	meta := page.EncodeMetaPage(encode.Header{
		FileType:            encode.FileTypeMain,
		FormatVersion:       encode.FormatVersion,
		Sequence:            0,
		WalCheckpointOffset: encode.HeaderSize,
		PageSize:            page.Size,
		PageCount:           1,
	})
	if err := os.WriteFile(path, meta, 0o644); err != nil {
		t.Fatal(err)
	}

	db2, err := dorradb.Open(path, dorradb.Options{
		WALCheckpointBytes: 1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	val, err := db2.Get([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "1" {
		t.Fatalf("got %q", val)
	}
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.dorradb")
	db, err := dorradb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_ = db.Set([]byte("a"), []byte("1"))
	if err := db.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	_, err = db.Get([]byte("a"))
	if err != dorradb.ErrNotFound {
		t.Fatalf("err=%v", err)
	}
}

func TestMainFileIsPaged(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.dorradb")
	db, err := dorradb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
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
