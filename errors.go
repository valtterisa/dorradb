package dorradb

import "errors"

var (
	ErrNotFound           = errors.New("dorradb: key not found")
	ErrInvalidKey         = errors.New("dorradb: invalid key")
	ErrClosed             = errors.New("dorradb: database closed")
	ErrLocked             = errors.New("dorradb: database locked by another process")
	ErrTooLarge           = errors.New("dorradb: key or value too large")
	ErrCorrupt            = errors.New("dorradb: corrupt data")
	ErrUnsupportedVersion = errors.New("dorradb: unsupported format version")
)
