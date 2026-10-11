package vfs

import (
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
)

// osFS is the default FS: thin wrappers around the Go os package + gofrs/flock.
type osFS struct{}

// osFile embeds *os.File so it satisfies vfs.File (ReadAt, WriteAt, Sync, …).
type osFile struct {
	*os.File
}

func (osFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (osFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (osFS) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (osFS) Remove(name string) error {
	return os.Remove(name)
}

// SyncDir flushes the directory entry for path (after rename of a checkpoint).
// Actual syscalls live in sync_unix.go / sync_windows.go.
func (osFS) SyncDir(path string) error {
	return syncDir(filepath.Dir(path))
}

type osLock struct {
	f *flock.Flock
}

// Lock tries to take an exclusive non-blocking lock on path.
// gofrs/flock uses flock on Unix and LockFileEx on Windows.
func (osFS) Lock(path string) (Lock, error) {
	f := flock.New(path)
	ok, err := f.TryLock()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrLocked
	}
	return &osLock{f: f}, nil
}

func (l *osLock) Unlock() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := l.f.Unlock()
	l.f = nil
	return err
}
