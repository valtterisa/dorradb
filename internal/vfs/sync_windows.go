//go:build windows

package vfs

import (
	"os"
	"syscall"
)

// syncDir tries to flush the directory on Windows. Directory FlushFileBuffers
// is not always supported; if it fails we ignore the error (NTFS rename is
// still reasonably durable for this MVP).
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
