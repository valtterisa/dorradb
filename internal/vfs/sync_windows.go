//go:build windows

package vfs

import (
	"os"
	"syscall"
)

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	err = syscall.Fsync(syscall.Handle(d.Fd()))
	if err != nil {
		return nil
	}
	return nil
}
