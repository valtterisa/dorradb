// Package index is the in-RAM map from key → version chain of on-disk Locs.
// Values themselves stay in pages; we only store page addresses here.
package index

import "sync"

// Loc points at one record in a heap page (page id + slot).
type Loc struct {
	PageID       uint32
	Slot         uint16
	OverflowPage uint32 // 0 if value is inline on the heap page
	Bytes        int    // approx on-disk size for stats
	Sequence     uint64 // logical version of this entry
	Deleted      bool   // tombstone until vacuum / prune
}

// version is one link in a newest-first chain.
type version struct {
	loc  Loc
	prev *version // older version
}

// Index is key → newest version (chain of older Locs). Safe for concurrent
// readers via RLock; writers take Lock on Apply/prune.
type Index struct {
	mu sync.RWMutex
	m  map[string]*version
}

func New() *Index {
	return &Index{m: make(map[string]*version)}
}

// Get returns the newest Loc for key (not snapshot-aware).
func (idx *Index) Get(key []byte) (Loc, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	v := idx.m[string(key)]
	if v == nil {
		return Loc{}, false
	}
	return v.loc, true
}

// GetVisible returns the newest Loc with Sequence <= snapshotSequence.
// If that Loc is a tombstone, ok is false.
func (idx *Index) GetVisible(key []byte, snapshotSequence uint64) (Loc, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for v := idx.m[string(key)]; v != nil; v = v.prev {
		if v.loc.Sequence > snapshotSequence {
			continue
		}
		if v.loc.Deleted {
			return Loc{}, false
		}
		return v.loc, true
	}
	return Loc{}, false
}

func (idx *Index) Put(key []byte, loc Loc) {
	idx.Apply(key, loc)
}

func (idx *Index) Delete(key []byte, sequence uint64) {
	idx.Apply(key, Loc{Sequence: sequence, Deleted: true})
}

// Apply pushes a newer Loc onto the version chain. Ignores Locs that are not
// strictly newer than the current head (recovery / replay safety).
func (idx *Index) Apply(key []byte, loc Loc) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	k := string(key)
	head := idx.m[k]
	if head != nil && head.loc.Sequence >= loc.Sequence {
		return
	}
	idx.m[k] = &version{loc: loc, prev: head}
}

func (idx *Index) LiveCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	n := 0
	for _, v := range idx.m {
		if v != nil && !v.loc.Deleted {
			n++
		}
	}
	return n
}

func (idx *Index) ApproxLiveBytes() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var n int64
	for _, v := range idx.m {
		if v != nil && !v.loc.Deleted {
			n += int64(v.loc.Bytes)
		}
	}
	return n
}

// ForEachLive visits newest non-deleted keys (used for stats / vacuum later).
func (idx *Index) ForEachLive(fn func(key string, loc Loc) error) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for k, v := range idx.m {
		if v == nil || v.loc.Deleted {
			continue
		}
		if err := fn(k, v.loc); err != nil {
			return err
		}
	}
	return nil
}

// Prune drops versions older than the Loc visible at horizon (keeps that Loc
// and everything newer). Safe when no reader can pin below horizon.
func (idx *Index) Prune(horizon uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for k, head := range idx.m {
		var visible *version
		for v := head; v != nil; v = v.prev {
			if v.loc.Sequence <= horizon {
				visible = v
				break
			}
		}
		if visible == nil {
			delete(idx.m, k)
			continue
		}
		var newer []Loc
		for v := head; v != visible; v = v.prev {
			newer = append(newer, v.loc)
		}
		newHead := &version{loc: visible.loc, prev: nil}
		for i := len(newer) - 1; i >= 0; i-- {
			newHead = &version{loc: newer[i], prev: newHead}
		}
		idx.m[k] = newHead
	}
}

func (idx *Index) Reset() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.m = make(map[string]*version)
}

func (idx *Index) MaxSequence() uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var max uint64
	for _, head := range idx.m {
		for v := head; v != nil; v = v.prev {
			if v.loc.Sequence > max {
				max = v.loc.Sequence
			}
		}
	}
	return max
}
