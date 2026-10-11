// Package vfs is DoraDB's Virtual File System — the only place the engine
// should touch "the disk". Storage/WAL/engine call these interfaces; the OS
// implementation (os.go + sync_*.go) maps them to Windows/Linux system calls.
//
// Why: keep Open/Get/Set the same everywhere, and isolate OS differences here
// (like SQLite's VFS), including a possible future in-memory FS for tests.
package vfs

import (
	"errors"
	"io"
	"os"
)

// ErrLocked means another process already holds the exclusive DB lock.
var ErrLocked = errors.New("vfs: resource locked")

// File is one open file (main DB, WAL, temp compact file, …).
// Sync flushes file data to stable storage (fsync / FlushFileBuffers).
type File interface {
	io.ReaderAt
	io.WriterAt
	io.Writer
	io.Closer
	Sync() error
	Truncate(size int64) error
	Seek(offset int64, whence int) (int64, error)
	Stat() (os.FileInfo, error)
}

// Lock is an exclusive advisory lock. Unlock (or process exit) releases it.
type Lock interface {
	Unlock() error
}

// FS is the filesystem the engine uses for all durable I/O.
type FS interface {
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	MkdirAll(path string, perm os.FileMode) error
	Rename(oldpath, newpath string) error
	Remove(name string) error
	// SyncDir fsyncs the parent directory of path so a rename is durable
	// after power loss (important on Linux/Unix).
	SyncDir(path string) error
	// Lock takes an exclusive lock on path (we use path+".lock").
	Lock(path string) (Lock, error)
}

// OS returns the real-disk VFS for the current operating system.
func OS() FS {
	return osFS{}
}
