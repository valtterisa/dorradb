package lock

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

var ErrLocked = errors.New("lock: held by another process")

type FileLock struct {
	path string
	f    *os.File
}

func PathFor(dbPath string) string {
	return dbPath + ".lock"
}

func Acquire(dbPath string) (*FileLock, error) {
	lockPath := PathFor(dbPath)
	for i := 0; i < 3; i++ {
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			content := fmt.Sprintf("%d\n%d\n", os.Getpid(), time.Now().UnixNano())
			if _, werr := f.WriteString(content); werr != nil {
				f.Close()
				os.Remove(lockPath)
				return nil, werr
			}
			if serr := f.Sync(); serr != nil {
				f.Close()
				os.Remove(lockPath)
				return nil, serr
			}
			return &FileLock{path: lockPath, f: f}, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}
		stale, rerr := isStale(lockPath)
		if rerr != nil {
			return nil, rerr
		}
		if !stale {
			return nil, ErrLocked
		}
		_ = os.Remove(lockPath)
	}
	return nil, ErrLocked
}

func isStale(lockPath string) (bool, error) {
	b, err := os.ReadFile(lockPath)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) < 1 {
		return true, nil
	}
	pid, err := strconv.Atoi(strings.TrimSpace(lines[0]))
	if err != nil || pid <= 0 {
		return true, nil
	}
	if pid == os.Getpid() {
		return true, nil
	}
	alive, err := pidAlive(pid)
	if err != nil {
		return false, err
	}
	return !alive, nil
}

func (l *FileLock) Release() error {
	if l == nil {
		return nil
	}
	var err error
	if l.f != nil {
		err = l.f.Close()
		l.f = nil
	}
	if rerr := os.Remove(l.path); rerr != nil && !os.IsNotExist(rerr) && err == nil {
		err = rerr
	}
	return err
}
