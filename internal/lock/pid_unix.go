//go:build unix

package lock

import (
	"os"
	"syscall"
)

func pidAlive(pid int) (bool, error) {
	p, err := os.FindProcess(pid)
	if err != nil {
		return false, nil
	}
	err = p.Signal(syscall.Signal(0))
	if err == nil {
		return true, nil
	}
	if err == syscall.ESRCH {
		return false, nil
	}
	if err == syscall.EPERM {
		return true, nil
	}
	return false, nil
}
