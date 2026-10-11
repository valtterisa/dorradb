//go:build unix

package vfs

import "os"

// syncDir fsyncs a directory so renames inside it survive power loss.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
