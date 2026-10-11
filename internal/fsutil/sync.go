package fsutil

import (
	"os"
	"path/filepath"
)

func SyncFile(f *os.File) error {
	return f.Sync()
}

func SyncDir(path string) error {
	dir := filepath.Dir(path)
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return syncDir(d)
}
