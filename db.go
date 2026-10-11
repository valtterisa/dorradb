package dorradb

import (
	"context"
	"errors"

	"dorradb/internal/encode"
	"dorradb/internal/engine"
	"dorradb/internal/vfs"
)

type DB struct {
	eng  *engine.Engine
	opts Options
}

func Open(path string) (*DB, error) {
	return OpenOptions(Options{Path: path})
}

func OpenOptions(opts Options) (*DB, error) {
	opts = opts.withDefaults()
	if opts.Path == "" {
		return nil, errors.New("dorradb: empty path")
	}
	eng, err := engine.Open(engine.Options{
		Path:                  opts.Path,
		WALCheckpointBytes:    opts.WALCheckpointBytes,
		WALCheckpointInterval: opts.WALCheckpointInterval,
		SyncWAL:               opts.syncWAL(),
		GroupCommitMaxOps:     opts.GroupCommitMaxOps,
		GroupCommitMaxWait:    opts.GroupCommitMaxWait,
		MaxKeyBytes:           opts.MaxKeyBytes,
		MaxValueBytes:         opts.MaxValueBytes,
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return &DB{eng: eng, opts: opts}, nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if err := db.checkKey(key); err != nil {
		return nil, err
	}
	val, err := db.eng.Get(key)
	return val, mapErr(err)
}

func (db *DB) Set(key, value []byte) error {
	if err := db.checkKey(key); err != nil {
		return err
	}
	if len(value) > db.opts.MaxValueBytes {
		return ErrTooLarge
	}
	return mapErr(db.eng.Set(key, value))
}

func (db *DB) Delete(key []byte) error {
	if err := db.checkKey(key); err != nil {
		return err
	}
	return mapErr(db.eng.Delete(key))
}

func (db *DB) Has(key []byte) (bool, error) {
	if err := db.checkKey(key); err != nil {
		return false, err
	}
	ok, err := db.eng.Has(key)
	return ok, mapErr(err)
}

func (db *DB) Checkpoint(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return mapErr(db.eng.Checkpoint(ctx))
}

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

func (db *DB) Close() error {
	return mapErr(db.eng.Close())
}

func (db *DB) checkKey(key []byte) error {
	if len(key) == 0 {
		return ErrInvalidKey
	}
	if len(key) > db.opts.MaxKeyBytes {
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
