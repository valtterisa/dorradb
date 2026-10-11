package dorradb

import (
	"context"
	"errors"

	"dorradb/internal/encode"
	"dorradb/internal/engine"
	"dorradb/internal/vfs"
)

const (
	maxKeyBytes   = 1 << 10 // 1 KiB
	maxValueBytes = 4 << 20 // 4 MiB
)

// DB is the public handle. All methods are safe to call from multiple goroutines;
// the engine serializes mutations on one writer loop.
type DB struct {
	eng *engine.Engine
}

// Open creates or opens a database at path (SQLite-style file).
// Also creates path-wal and path.lock as needed.
func Open(path string) (*DB, error) {
	if path == "" {
		return nil, errors.New("dorradb: empty path")
	}
	eng, err := engine.Open(path)
	if err != nil {
		return nil, mapErr(err)
	}
	return &DB{eng: eng}, nil
}

// Get returns a copy of the value for key, or ErrNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	if err := checkKey(key); err != nil {
		return nil, err
	}
	val, err := db.eng.Get(key)
	return val, mapErr(err)
}

// Set creates or overwrites key. Durable after return (WAL is fsynced).
func (db *DB) Set(key, value []byte) error {
	if err := checkKey(key); err != nil {
		return err
	}
	if len(value) > maxValueBytes {
		return ErrTooLarge
	}
	return mapErr(db.eng.Set(key, value))
}

// Delete removes key. Missing keys are not an error.
func (db *DB) Delete(key []byte) error {
	if err := checkKey(key); err != nil {
		return err
	}
	return mapErr(db.eng.Delete(key))
}

// Has reports whether key exists (no value copy).
func (db *DB) Has(key []byte) (bool, error) {
	if err := checkKey(key); err != nil {
		return false, err
	}
	ok, err := db.eng.Has(key)
	return ok, mapErr(err)
}

// Checkpoint forces a compact of live keys into the main file and truncates the WAL.
func (db *DB) Checkpoint(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return mapErr(db.eng.Checkpoint(ctx))
}

// Stats returns sizes and counters (see Stats fields).
func (db *DB) Stats() Stats {
	s := db.eng.Stats()
	return Stats{
		Path:            s.Path,
		Seq:             s.Seq,
		LiveKeys:        s.LiveKeys,
		MainBytes:       s.MainBytes,
		WALBytes:        s.WALBytes,
		ApproxDeadBytes: s.ApproxDeadBytes,
		LastCheckpoint:  s.LastCheckpoint,
	}
}

// Close flushes, releases the file lock, and closes files. Safe to call once.
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
