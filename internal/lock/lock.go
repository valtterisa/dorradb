package lock

import (
	"errors"

	"github.com/gofrs/flock"
)

var ErrLocked = errors.New("lock: held by another process")

type FileLock struct {
	path string
	f    *flock.Flock
}

func PathFor(dbPath string) string {
	return dbPath + ".lock"
}

func Acquire(dbPath string) (*FileLock, error) {
	lockPath := PathFor(dbPath)
	f := flock.New(lockPath)
	ok, err := f.TryLock()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrLocked
	}
	return &FileLock{path: lockPath, f: f}, nil
}

func (l *FileLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := l.f.Unlock()
	l.f = nil
	return err
}
