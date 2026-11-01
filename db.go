package dorradb

import (
	"errors"
	"sync"

	"dorradb/internal/encode"
	"dorradb/internal/engine"
	"dorradb/internal/storage/page"
	"dorradb/internal/vfs"
)

const maxValueBytes = 4 << 20 // 4 MiB (row record / cell value)

// DB is the public handle. All methods are safe to call from multiple goroutines.
// Reads and writes run in transactions (AUTOCOMMIT for Exec/Query, or explicit
// Begin/Commit/Rollback). One writable tx at a time; reads use MVCC snapshots.
type DB struct {
	eng     *engine.Engine
	writeMu sync.Mutex // at most one writable Tx
}

// Open creates or opens a database at path (SQLite-style file).
// Also creates path-wal and path.lock as needed.
//
// Optional opts overrides defaults (see Options and DefaultOptions). Only the
// first Options value is used.
//
//	dorradb.Open("app.dorradb")
//	dorradb.Open("app.dorradb", dorradb.Options{DisableSync: true})
func Open(path string, opts ...Options) (*DB, error) {
	if path == "" {
		return nil, errors.New("dorradb: empty path")
	}
	var o Options
	if len(opts) > 0 {
		o = opts[0]
	}
	eng, err := engine.Open(path, o.toEngine())
	if err != nil {
		return nil, mapErr(err)
	}
	return &DB{eng: eng}, nil
}

// Stats returns sizes and counters (see Stats fields).
func (db *DB) Stats() Stats {
	s := db.eng.Stats()
	return Stats{
		Path:              s.Path,
		Sequence:          s.Sequence,
		CommittedSequence: s.CommittedSequence,
		PinnedReaders:     s.PinnedReaders,
		LiveKeys:          s.LiveKeys,
		MainBytes:         s.MainBytes,
		WALBytes:          s.WALBytes,
		ApproxDeadBytes:   s.ApproxDeadBytes,
		CachePages:        s.CachePages,
		DirtyFrames:       s.DirtyFrames,
	}
}

// Close flushes, releases the file lock, and closes files. Safe to call once.
// Waits for any open transaction pins to be released.
func (db *DB) Close() error {
	return mapErr(db.eng.Close())
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, engine.ErrNotFound):
		return ErrNotFound
	case errors.Is(err, engine.ErrClosed):
		return ErrClosed
	case errors.Is(err, engine.ErrTableNotFound):
		return ErrTableNotFound
	case errors.Is(err, engine.ErrTableExists):
		return ErrTableExists
	case errors.Is(err, vfs.ErrLocked):
		return ErrLocked
	case errors.Is(err, encode.ErrVersion):
		return ErrUnsupportedVersion
	case errors.Is(err, encode.ErrMidCRC), errors.Is(err, encode.ErrCRC), errors.Is(err, encode.ErrMagic), errors.Is(err, encode.ErrFileType),
		errors.Is(err, page.ErrCorrupt), errors.Is(err, page.ErrShort):
		return ErrCorrupt
	default:
		return err
	}
}
