// Package harness provides temp-DB helpers, crash simulation, and FaultFS for
// DorraDB reliability tests.
package harness

import (
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"dorradb"
	"dorradb/internal/engine"
	"dorradb/internal/vfs"
)

// Open opens a new DB under t.TempDir with the given options.
func Open(t testing.TB, opts dorradb.Options) (*dorradb.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "app.dorradb")
	db, err := dorradb.Open(path, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db, path
}

// OpenEngine opens an engine with an injectable FS (for FaultFS tests).
func OpenEngine(t testing.TB, fs vfs.FS, opts engine.Options) (*engine.Engine, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "app.dorradb")
	eng, err := engine.OpenWithFS(path, fs, opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	return eng, path
}

// CrashCopy copies path and path-wal into a fresh temp dir while the source DB
// may still be open. The copy captures durable WAL state and a possibly lagged
// main file (dirty pages only in memory are not copied). Returns the copy path.
func CrashCopy(t testing.TB, path string) string {
	t.Helper()
	dstDir := t.TempDir()
	dst := filepath.Join(dstDir, filepath.Base(path))
	for _, suffix := range []string{"", "-wal"} {
		src := path + suffix
		data, err := os.ReadFile(src)
		if err != nil {
			if os.IsNotExist(err) && suffix == "-wal" {
				continue
			}
			t.Fatalf("crash copy read %s: %v", src, err)
		}
		if err := os.WriteFile(dst+suffix, data, 0o644); err != nil {
			t.Fatalf("crash copy write %s: %v", dst+suffix, err)
		}
	}
	return dst
}

// AppendWALGarbage appends raw bytes to the WAL (simulates a torn/garbage tail).
func AppendWALGarbage(t testing.TB, path string, garbage []byte) {
	t.Helper()
	walPath := path + "-wal"
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer f.Close()
	if _, err := f.Write(garbage); err != nil {
		t.Fatalf("append garbage: %v", err)
	}
}

// TruncateFile truncates path to size bytes.
func TruncateFile(t testing.TB, path string, size int64) {
	t.Helper()
	if err := os.Truncate(path, size); err != nil {
		t.Fatalf("truncate %s: %v", path, err)
	}
}

// FaultFS wraps an FS and can fail the Nth write/sync or drop unsynced data.
type FaultFS struct {
	Inner vfs.FS

	mu sync.Mutex

	// FailWriteAfter fails the (n+1)th Write/WriteAt (0 = fail first write).
	// -1 disables.
	FailWriteAfter int
	// FailSyncAfter fails the (n+1)th Sync. -1 disables.
	FailSyncAfter int
	// FailFlushSyncAfter fails Sync only on the main DB file (not WAL). -1 disables.
	FailMainSyncAfter int

	writeCount int
	syncCount  int
	mainSyncs  int

	// track unsynced writes per file for CrashDropUnsynced
	files map[string]*faultFile
}

// NewFaultFS wraps inner (defaults to vfs.OS()). Fault injection is off (-1).
func NewFaultFS(inner vfs.FS) *FaultFS {
	if inner == nil {
		inner = vfs.OS()
	}
	return &FaultFS{
		Inner:             inner,
		FailWriteAfter:    -1,
		FailSyncAfter:     -1,
		FailMainSyncAfter: -1,
		files:             make(map[string]*faultFile),
	}
}

func (f *FaultFS) SyncCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.syncCount
}

func (f *FaultFS) WriteCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writeCount
}

func (f *FaultFS) OpenFile(name string, flag int, perm os.FileMode) (vfs.File, error) {
	inner, err := f.Inner.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	ff := &faultFile{fs: f, name: name, File: inner, isWAL: len(name) > 4 && name[len(name)-4:] == "-wal"}
	f.mu.Lock()
	f.files[name] = ff
	f.mu.Unlock()
	return ff, nil
}

func (f *FaultFS) MkdirAll(path string, perm os.FileMode) error {
	return f.Inner.MkdirAll(path, perm)
}

func (f *FaultFS) Rename(oldpath, newpath string) error {
	return f.Inner.Rename(oldpath, newpath)
}

func (f *FaultFS) Remove(name string) error {
	return f.Inner.Remove(name)
}

func (f *FaultFS) SyncDir(path string) error {
	return f.Inner.SyncDir(path)
}

func (f *FaultFS) Lock(path string) (vfs.Lock, error) {
	return f.Inner.Lock(path)
}

type faultFile struct {
	fs    *FaultFS
	name  string
	isWAL bool
	vfs.File

	mu       sync.Mutex
	unsynced []unsyncedWrite
	size     int64
	sizeInit bool
}

type unsyncedWrite struct {
	off  int64
	data []byte
}

func (ff *faultFile) ensureSize() error {
	if ff.sizeInit {
		return nil
	}
	info, err := ff.File.Stat()
	if err != nil {
		return err
	}
	ff.size = info.Size()
	ff.sizeInit = true
	return nil
}

func (ff *faultFile) Write(p []byte) (int, error) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	if err := ff.ensureSize(); err != nil {
		return 0, err
	}
	return ff.writeAtLocked(p, ff.size)
}

func (ff *faultFile) WriteAt(p []byte, off int64) (int, error) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return ff.writeAtLocked(p, off)
}

func (ff *faultFile) writeAtLocked(p []byte, off int64) (int, error) {
	ff.fs.mu.Lock()
	n := ff.fs.writeCount
	ff.fs.writeCount++
	failAfter := ff.fs.FailWriteAfter
	ff.fs.mu.Unlock()

	if failAfter >= 0 && n == failAfter {
		return 0, io.ErrShortWrite
	}
	nw, err := ff.File.WriteAt(p, off)
	if err != nil {
		return nw, err
	}
	cp := append([]byte(nil), p[:nw]...)
	ff.unsynced = append(ff.unsynced, unsyncedWrite{off: off, data: cp})
	end := off + int64(nw)
	if end > ff.size {
		ff.size = end
	}
	return nw, nil
}

func (ff *faultFile) Sync() error {
	ff.fs.mu.Lock()
	n := ff.fs.syncCount
	ff.fs.syncCount++
	failAfter := ff.fs.FailSyncAfter
	mainFail := ff.fs.FailMainSyncAfter
	mainN := ff.fs.mainSyncs
	if !ff.isWAL {
		ff.fs.mainSyncs++
	}
	ff.fs.mu.Unlock()

	if failAfter >= 0 && n == failAfter {
		return os.ErrInvalid
	}
	if !ff.isWAL && mainFail >= 0 && mainN == mainFail {
		return os.ErrInvalid
	}

	ff.mu.Lock()
	ff.unsynced = nil
	ff.mu.Unlock()
	return ff.File.Sync()
}

func (ff *faultFile) Truncate(size int64) error {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	if err := ff.File.Truncate(size); err != nil {
		return err
	}
	ff.size = size
	ff.sizeInit = true
	// Drop unsynced writes past new size.
	kept := ff.unsynced[:0]
	for _, w := range ff.unsynced {
		if w.off < size {
			kept = append(kept, w)
		}
	}
	ff.unsynced = kept
	return nil
}

// DropUnsynced truncates each open file to discard bytes written since the last Sync.
// Call while the engine is still "crashed" (do not Sync). Used after poisoning /
// for power-loss simulation when FailSyncAfter triggers.
func (f *FaultFS) DropUnsynced() error {
	f.mu.Lock()
	files := make([]*faultFile, 0, len(f.files))
	for _, ff := range f.files {
		files = append(files, ff)
	}
	f.mu.Unlock()

	for _, ff := range files {
		ff.mu.Lock()
		if len(ff.unsynced) == 0 {
			ff.mu.Unlock()
			continue
		}
		// Find minimum offset of unsynced writes and truncate there.
		minOff := ff.unsynced[0].off
		for _, w := range ff.unsynced[1:] {
			if w.off < minOff {
				minOff = w.off
			}
		}
		ff.unsynced = nil
		err := ff.File.Truncate(minOff)
		ff.size = minOff
		ff.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}
