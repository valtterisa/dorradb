//go:build !unix && !windows

package fsutil

import "os"

func syncDir(d *os.File) error {
	return d.Sync()
}
