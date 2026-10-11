//go:build windows

package fsutil

import (
	"os"
	"syscall"
)

func syncDir(d *os.File) error {
	err := syscall.Fsync(syscall.Handle(d.Fd()))
	if err == nil {
		return nil
	}
	// Directory FlushFileBuffers is not always supported; rename is still durable enough on NTFS for MVP.
	return nil
}
