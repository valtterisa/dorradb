package recovery_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dorradb"
	"dorradb/internal/encode"
	"dorradb/internal/engine"
	"dorradb/internal/storage/page"
	"dorradb/tests/harness"
)

func TestDoubleCrashGarbageWALTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.dorradb")
	db, err := dorradb.Open(path, dorradb.Options{
		WALCheckpointBytes: 1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	harness.AppendWALGarbage(t, path, []byte{0xde, 0xad, 0xbe, 0xef, 0x00, 0x01})

	db2, err := dorradb.Open(path, dorradb.Options{WALCheckpointBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	if err := db2.Set([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}

	harness.AppendWALGarbage(t, path, []byte{0xaa, 0xbb, 0xcc, 0xdd})

	db3, err := dorradb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db3.Close()

	for _, tc := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}} {
		got, err := db3.Get([]byte(tc.k))
		if err != nil {
			t.Fatalf("get %s: %v", tc.k, err)
		}
		if string(got) != tc.v {
			t.Fatalf("%s: got %q want %q", tc.k, got, tc.v)
		}
	}
}

func TestCrashCopyPreservesAckedData(t *testing.T) {
	db, path := harness.Open(t, dorradb.Options{WALCheckpointBytes: 1 << 30})
	if err := db.Set([]byte("x"), []byte("durable")); err != nil {
		t.Fatal(err)
	}
	copyPath := harness.CrashCopy(t, path)
	_ = db.Close()

	db2, err := dorradb.Open(copyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	got, err := db2.Get([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "durable" {
		t.Fatalf("got %q", got)
	}
}

func TestRecoveryFromWALWhenMainLags(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.dorradb")
	db, err := dorradb.Open(path, dorradb.Options{WALCheckpointBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("x"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

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

	db2, err := dorradb.Open(path, dorradb.Options{WALCheckpointBytes: 1 << 30})
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

func TestPoisonOnWALSyncFailure(t *testing.T) {
	fs := harness.NewFaultFS(nil)
	eng, _ := harness.OpenEngine(t, fs, engine.Options{SyncWAL: true, CachePages: 8})
	// Fail the next Sync (commit's WAL Sync).
	fs.FailSyncAfter = fs.SyncCount()

	err := eng.Commit([]engine.Mutation{{Key: []byte("k"), Value: []byte("v")}})
	if !errors.Is(err, engine.ErrFatal) {
		t.Fatalf("commit err=%v want ErrFatal (syncs so far=%d)", err, fs.SyncCount())
	}
	err2 := eng.Commit([]engine.Mutation{{Key: []byte("k2"), Value: []byte("v2")}})
	if !errors.Is(err2, engine.ErrFatal) {
		t.Fatalf("second commit err=%v want ErrFatal", err2)
	}
	if eng.CommittedSequence() != 0 {
		t.Fatalf("committedSequence=%d want 0", eng.CommittedSequence())
	}
	_ = eng.Close()
}

func TestPoisonOnWALWriteFailure(t *testing.T) {
	fs := harness.NewFaultFS(nil)
	eng, _ := harness.OpenEngine(t, fs, engine.Options{SyncWAL: false, CachePages: 8})
	fs.FailWriteAfter = fs.WriteCount() // fail next WriteAt (first mutation frame)

	err := eng.Commit([]engine.Mutation{{Key: []byte("k"), Value: []byte("v")}})
	if !errors.Is(err, engine.ErrFatal) {
		t.Fatalf("err=%v want ErrFatal (writes=%d)", err, fs.WriteCount())
	}
	if !errors.Is(eng.Err(), engine.ErrFatal) {
		t.Fatalf("eng.Err=%v", eng.Err())
	}
	_ = eng.Close()
}

func TestUncommittedBufferGoneAfterCrashCopy(t *testing.T) {
	db, path := harness.Open(t, dorradb.Options{WALCheckpointBytes: 1 << 30})
	if err := db.Set([]byte("stable"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set([]byte("pending"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	copyPath := harness.CrashCopy(t, path)
	_ = tx.Rollback()
	_ = db.Close()

	db2, err := dorradb.Open(copyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if _, err := db2.Get([]byte("stable")); err != nil {
		t.Fatal(err)
	}
	_, err = db2.Get([]byte("pending"))
	if !errors.Is(err, dorradb.ErrNotFound) {
		t.Fatalf("pending should be absent, err=%v", err)
	}
}

func TestMultiKeyCommitAtomicOnCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.dorradb")

	db, err := dorradb.Open(path, dorradb.Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	walPath := path + "-wal"
	var frames []byte
	frames = append(frames, encode.EncodeHeader(encode.Header{
		FileType:      encode.FileTypeWAL,
		FormatVersion: encode.FormatVersion,
	})...)
	frames = append(frames, encode.EncodeFrame(encode.Record{Sequence: 1, Op: encode.OpSet, Key: []byte("a"), Value: []byte("1")})...)
	frames = append(frames, encode.EncodeFrame(encode.Record{Sequence: 1, Op: encode.OpCommit})...)
	frames = append(frames, encode.EncodeFrame(encode.Record{Sequence: 2, Op: encode.OpSet, Key: []byte("b"), Value: []byte("2")})...)
	frames = append(frames, encode.EncodeFrame(encode.Record{Sequence: 3, Op: encode.OpSet, Key: []byte("c"), Value: []byte("3")})...)
	frames = append(frames, encode.EncodeFrame(encode.Record{Sequence: 4, Op: encode.OpSet, Key: []byte("d"), Value: []byte("4")})...)
	if err := os.WriteFile(walPath, frames, 0o644); err != nil {
		t.Fatal(err)
	}

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

	db2, err := dorradb.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	got, err := db2.Get([]byte("a"))
	if err != nil || string(got) != "1" {
		t.Fatalf("a: %q %v", got, err)
	}
	for _, k := range []string{"b", "c", "d"} {
		_, err := db2.Get([]byte(k))
		if !errors.Is(err, dorradb.ErrNotFound) {
			t.Fatalf("%s should be absent: %v", k, err)
		}
	}
}

func TestDeprecatedGroupCommitOptionsIgnored(t *testing.T) {
	start := time.Now()
	db, _ := harness.Open(t, dorradb.Options{
		GroupCommitMaxOps:  1,
		GroupCommitMaxWait: time.Second,
		DisableSync:        true,
	})
	defer db.Close()
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("Set took too long; GroupCommitMaxWait may not be ignored")
	}
}
