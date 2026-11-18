package dorradb

import "errors"

// Public errors callers can check with errors.Is.
var (
	ErrNotFound           = errors.New("dorradb: not found")
	ErrClosed             = errors.New("dorradb: database closed")
	ErrLocked             = errors.New("dorradb: database locked by another process")
	ErrTooLarge           = errors.New("dorradb: value too large")
	ErrCorrupt            = errors.New("dorradb: corrupt data")
	ErrUnsupportedVersion = errors.New("dorradb: unsupported format version")
	ErrTxClosed           = errors.New("dorradb: transaction closed")
	ErrTxNotWritable      = errors.New("dorradb: transaction is read-only")
	ErrTableNotFound      = errors.New("dorradb: table not found")
	ErrTableExists        = errors.New("dorradb: table already exists")
	ErrConstraint         = errors.New("dorradb: constraint failed")
)
