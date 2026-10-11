package vfs

import (
	"errors"
	"io"
	"os"
)

var ErrLocked = errors.New("vfs: resource locked")

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

type Lock interface {
	Unlock() error
}

type FS interface {
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	MkdirAll(path string, perm os.FileMode) error
	Rename(oldpath, newpath string) error
	Remove(name string) error
	SyncDir(path string) error
	Lock(path string) (Lock, error)
}

func OS() FS {
	return osFS{}
}
