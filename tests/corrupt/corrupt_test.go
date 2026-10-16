package corrupt_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"dorradb"
	"dorradb/internal/encode"
	"dorradb/internal/storage/page"
	"dorradb/tests/harness"
)

func TestUnsupportedVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.dorradb")
	// EncodeMetaPage forces current FormatVersion; craft header bytes directly.
	h := encode.Header{
		FileType:            encode.FileTypeMain,
		FormatVersion:       encode.FormatVersion + 1,
		WalCheckpointOffset: encode.HeaderSize,
		PageSize:            page.Size,
		PageCount:           1,
	}
	meta := make([]byte, page.Size)
	copy(meta, encode.EncodeHeader(h))
	meta[encode.HeaderSize] = page.TypeMeta
	if err := os.WriteFile(path, meta, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := dorradb.Open(path)
	if !errors.Is(err, dorradb.ErrUnsupportedVersion) {
		t.Fatalf("err=%v want ErrUnsupportedVersion", err)
	}
}

func TestBadMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.dorradb")
	buf := make([]byte, page.Size)
	copy(buf, []byte("XXXX"))
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := dorradb.Open(path)
	if !errors.Is(err, dorradb.ErrCorrupt) {
		t.Fatalf("err=%v want ErrCorrupt", err)
	}
}

func TestV2WALOpensAndUpgrades(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.dorradb")

	// Main: empty v2 meta (EncodeMetaPage would force current version).
	h := encode.Header{
		FileType:            encode.FileTypeMain,
		FormatVersion:       2,
		Sequence:            0,
		WalCheckpointOffset: encode.HeaderSize,
		PageSize:            page.Size,
		PageCount:           1,
	}
	meta := make([]byte, page.Size)
	copy(meta, encode.EncodeHeader(h))
	meta[encode.HeaderSize] = page.TypeMeta
	if err := os.WriteFile(path, meta, 0o644); err != nil {
		t.Fatal(err)
	}

	// WAL v2 with self-committed frames (no OpCommit).
	var wal []byte
	wal = append(wal, encode.EncodeHeader(encode.Header{
		FileType:      encode.FileTypeWAL,
		FormatVersion: 2,
	})...)
	wal = append(wal, encode.EncodeFrame(encode.Record{
		Sequence: 1, Op: encode.OpSet, Key: []byte("legacy"), Value: []byte("ok"),
	})...)
	if err := os.WriteFile(path+"-wal", wal, 0o644); err != nil {
		t.Fatal(err)
	}

	db, err := dorradb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.Get([]byte("legacy"))
	if err != nil || string(got) != "ok" {
		t.Fatalf("got %q %v", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// After recover checkpoint, files should be v3.
	hdr := make([]byte, encode.HeaderSize)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.ReadAt(hdr, 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	decoded, err := encode.DecodeHeader(hdr)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.FormatVersion != encode.FormatVersion {
		t.Fatalf("main version=%d want %d", decoded.FormatVersion, encode.FormatVersion)
	}

	// Still readable.
	db2, err := dorradb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	got, err = db2.Get([]byte("legacy"))
	if err != nil || string(got) != "ok" {
		t.Fatalf("reopen: %q %v", got, err)
	}
}

func TestGroupCommitOptionsIgnored(t *testing.T) {
	db, _ := harness.Open(t, dorradb.Options{
		GroupCommitMaxOps: 1,
		DisableSync:       true,
	})
	defer db.Close()
	if err := db.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
}
