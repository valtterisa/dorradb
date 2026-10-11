//go:build !unix && !windows

package vfs

// syncDir is a no-op on platforms without a real directory fsync.
func syncDir(dir string) error {
	return nil
}
