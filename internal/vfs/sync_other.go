//go:build !unix && !windows

package vfs

func syncDir(dir string) error {
	return nil
}
