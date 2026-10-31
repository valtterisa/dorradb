package storage

import (
	"dorradb/internal/encode"
	"dorradb/internal/vfs"
)

// Store wraps the main-file buffer pool. Row data lives in the clustered B+ tree
// (see internal/btree); this type is the engine's handle for pool I/O and meta.
type Store struct {
	pool *BufferPool
}

// OpenStore opens the paged main file.
func OpenStore(fs vfs.FS, dbPath string, create bool, opts OpenOptions) (*Store, error) {
	p, err := Open(fs, dbPath, create, opts)
	if err != nil {
		return nil, err
	}
	return &Store{pool: p}, nil
}

func (s *Store) Path() string                    { return s.pool.Path() }
func (s *Store) Size() int64                     { return s.pool.Size() }
func (s *Store) Header() encode.Header           { return s.pool.Header() }
func (s *Store) SetHeader(h encode.Header) error { return s.pool.SetHeader(h) }
func (s *Store) FlushAll() error                 { return s.pool.FlushAll() }
func (s *Store) Close() error                    { return s.pool.Close() }
func (s *Store) CachePages() int                 { return s.pool.CachePages() }
func (s *Store) DirtyFrames() int                { return s.pool.DirtyFrames() }
func (s *Store) PageCount() uint32               { return s.pool.PageCount() }
func (s *Store) Pool() *BufferPool               { return s.pool }
