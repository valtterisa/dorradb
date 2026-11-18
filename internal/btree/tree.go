// Package btree is a clustered, versioned B+ tree on top of the buffer pool.
// Fill in the TODOs until `go test ./internal/btree` passes. harness_test.go explains
// the rules the engine expects.
package btree

import (
	"errors"
	"sync"

	"dorradb/internal/storage"
)

var ErrNotImplemented = errors.New("btree: not implemented")

// Tree is one B+ tree. Add whatever fields you need.
type Tree struct {
	mu     sync.RWMutex
	pool   *storage.BufferPool
	root   uint32
	flush  func() error       // call when the pool runs low on free frames
	onRoot func(uint32) error // call when the root page changes
}

// Open loads the tree whose root is in pool.Header().TableRoot, or creates an
// empty one (and stores its root there) if TableRoot is 0.
func Open(pool *storage.BufferPool) (*Tree, error) {
	// TODO
	return nil, ErrNotImplemented
}

// Attach opens an existing tree at root.
func Attach(pool *storage.BufferPool, root uint32, onRoot func(uint32) error) *Tree {
	// TODO
	return &Tree{}
}

// NewEmpty creates an empty tree and returns it with its root page id.
func NewEmpty(pool *storage.BufferPool, onRoot func(uint32) error) (*Tree, uint32, error) {
	// TODO
	return nil, 0, ErrNotImplemented
}

// SetFlusher replaces the function used to flush dirty pages.
func (t *Tree) SetFlusher(fn func() error) {
	// TODO
}

// Root returns the current root page id.
func (t *Tree) Root() uint32 {
	// TODO
	return 0
}

// GetVisible returns the newest value of key with sequence <= snap.
func (t *Tree) GetVisible(key []byte, snap uint64) ([]byte, bool, error) {
	// TODO
	return nil, false, ErrNotImplemented
}

// Put adds a new version of key at sequence.
func (t *Tree) Put(key, value []byte, sequence, horizon uint64) error {
	// TODO
	return ErrNotImplemented
}

// Delete adds a tombstone for key at sequence.
func (t *Tree) Delete(key []byte, sequence, horizon uint64) error {
	// TODO
	return ErrNotImplemented
}

// Prune drops versions no snapshot >= horizon can see. With forgetDeleted it
// also removes keys whose remaining newest version is a tombstone <= horizon.
func (t *Tree) Prune(horizon uint64, forgetDeleted bool) error {
	// TODO
	return ErrNotImplemented
}

// LiveCount counts keys whose newest version is not a tombstone.
func (t *Tree) LiveCount() int {
	// TODO
	return 0
}

// ApproxLiveBytes sums len(key)+len(newest value) over live keys.
func (t *Tree) ApproxLiveBytes() int64 {
	// TODO
	return 0
}

// Iterator walks keys in order, starting from Seek.
type Iterator struct {
	// TODO
}

// Seek returns an iterator positioned at the first key >= key.
func (t *Tree) Seek(key []byte) (*Iterator, error) {
	// TODO
	return nil, ErrNotImplemented
}

// Valid reports whether the iterator is on a key.
func (it *Iterator) Valid() bool {
	// TODO
	return false
}

// Err returns the first error the iterator hit.
func (it *Iterator) Err() error {
	// TODO
	return nil
}

// Key returns a copy of the current key.
func (it *Iterator) Key() ([]byte, error) {
	// TODO
	return nil, ErrNotImplemented
}

// VisibleValue returns the current key's value visible at snap.
func (it *Iterator) VisibleValue(snap uint64) ([]byte, bool, error) {
	// TODO
	return nil, false, ErrNotImplemented
}

// Next moves to the next key.
func (it *Iterator) Next() error {
	// TODO
	return ErrNotImplemented
}

// Close releases the iterator.
func (it *Iterator) Close() {
	// TODO
}
