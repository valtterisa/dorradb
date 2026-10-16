package dorradb

import "errors"

// Public errors callers can check with errors.Is.
var (
	ErrNotFound           = errors.New("dorradb: key not found")
	ErrInvalidKey         = errors.New("dorradb: invalid key")
	ErrClosed             = errors.New("dorradb: database closed")
	ErrLocked             = errors.New("dorradb: database locked by another process")
	ErrTooLarge           = errors.New("dorradb: key or value too large")
	ErrCorrupt            = errors.New("dorradb: corrupt data")
	ErrUnsupportedVersion = errors.New("dorradb: unsupported format version")
	ErrTxClosed           = errors.New("dorradb: transaction closed")
	ErrTxNotWritable      = errors.New("dorradb: transaction is read-only")
	// ErrFatal means an unrecoverable I/O error; Close and reopen to recover from WAL.
	ErrFatal = errors.New("dorradb: fatal I/O error; close and reopen")
)
