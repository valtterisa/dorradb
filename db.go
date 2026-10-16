package dorradb

import (
	"errors"
	"sync"

	"dorradb/internal/encode"
	"dorradb/internal/engine"
	"dorradb/internal/vfs"
)

const (
	maxKeyBytes   = 1 << 10 // 1 KiB
	maxValueBytes = 4 << 20 // 4 MiB
)

// DB is the public handle. All methods are safe to call from multiple goroutines.
// Every read and write runs inside a transaction (AUTOCOMMIT for Get/Set/Delete/Has,
// or explicit Begin/Commit/Rollback). One writable tx at a time; reads use MVCC
// snapshot isolation.
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

// Get returns a copy of the value for key, or ErrNotFound (AUTOCOMMIT read tx).
func (db *DB) Get(key []byte) ([]byte, error) {
	tx, err := db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return tx.Get(key)
}

// Set creates or overwrites key (AUTOCOMMIT write tx). Durable after return when
// WAL sync is enabled (default; see Options.DisableSync).
func (db *DB) Set(key, value []byte) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.Set(key, value); err != nil {
		return err
	}
	return tx.Commit()
}

// Delete removes key (AUTOCOMMIT write tx). Missing keys are not an error.
func (db *DB) Delete(key []byte) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.Delete(key); err != nil {
		return err
	}
	return tx.Commit()
}

// Has reports whether key exists (AUTOCOMMIT read tx).
func (db *DB) Has(key []byte) (bool, error) {
	tx, err := db.Begin(false)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	return tx.Has(key)
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

func checkKey(key []byte) error {
	if len(key) == 0 {
		return ErrInvalidKey
	}
	if len(key) > maxKeyBytes {
		return ErrTooLarge
	}
	return nil
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, engine.ErrFatal):
		return ErrFatal
	case errors.Is(err, engine.ErrNotFound):
		return ErrNotFound
	case errors.Is(err, engine.ErrClosed):
		return ErrClosed
	case errors.Is(err, vfs.ErrLocked):
		return ErrLocked
	case errors.Is(err, encode.ErrVersion):
		return ErrUnsupportedVersion
	case errors.Is(err, encode.ErrMidCRC), errors.Is(err, encode.ErrCRC), errors.Is(err, encode.ErrMagic), errors.Is(err, encode.ErrFileType):
		return ErrCorrupt
	default:
		return err
	}
}
