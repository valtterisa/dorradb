package btree

import (
	"sync"

	"dorradb/internal/storage"
	"dorradb/internal/storage/page"
)

// Tree is a clustered table B+ tree. One writer at a time (Put/Delete/Prune
// take the write lock); readers share a read lock, so they never see a page
// mid-rewrite.
//
// The buffer pool never writes dirty pages on its own, so the tree flushes
// (via the flusher) only at points where the whole structure is consistent:
// before a mutation touches tree pages, or between leaves during Prune.
type Tree struct {
	mu    sync.RWMutex
	pool  *storage.BufferPool
	root  uint32
	flush func() error
}

// Open loads or creates the table root from the pool header.
func Open(pool *storage.BufferPool) (*Tree, error) {
	t := &Tree{pool: pool, root: pool.Header().TableRoot}
	t.flush = pool.FlushAll
	if t.root == 0 {
		id, _, err := pool.NewPage(func(b []byte) { Init(b, page.TypeBTreeLeaf) })
		if err != nil {
			return nil, err
		}
		pool.Unpin(id, true)
		t.root = id
		if err := t.persistRoot(); err != nil {
			return nil, err
		}
	}
	return t, nil
}

// SetFlusher replaces the function used to flush dirty pages when the cache
// runs low. It must persist the current root (see Root) along with the pages.
func (t *Tree) SetFlusher(fn func() error) { t.flush = fn }

// Root is the current root page id. Writer-side only.
func (t *Tree) Root() uint32 { return t.root }

func (t *Tree) persistRoot() error {
	h := t.pool.Header()
	h.TableRoot = t.root
	return t.pool.SetHeader(h)
}

// reserve makes sure n frames are usable before a step that must not be
// interrupted. It keeps a quarter of the cache free for concurrent readers.
func (t *Tree) reserve(n int) error {
	want := n
	if h := t.pool.CachePages() / 4; h > want {
		want = h
	}
	if t.pool.FreeFrames() >= want {
		return nil
	}
	if err := t.flush(); err != nil {
		return err
	}
	if t.pool.FreeFrames() < n {
		return storage.ErrNoFreeFrame
	}
	return nil
}

// descend returns the internal path (root→parent, unpinned) and the pinned leaf.
func (t *Tree) descend(key []byte) (path []uint32, leafID uint32, leaf []byte, err error) {
	id := t.root
	for {
		buf, err := t.pool.Get(id)
		if err != nil {
			return nil, 0, nil, err
		}
		switch TypeOf(buf) {
		case page.TypeBTreeLeaf:
			return path, id, buf, nil
		case page.TypeBTreeInternal:
			child, err := SearchInternal(buf, key)
			t.pool.Unpin(id, false)
			if err != nil {
				return nil, 0, nil, err
			}
			path = append(path, id)
			id = child
		default:
			t.pool.Unpin(id, false)
			return nil, 0, nil, ErrBadType
		}
	}
}

// GetVisible returns the value visible at snapshot snap.
func (t *Tree) GetVisible(key []byte, snap uint64) ([]byte, bool, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	_, leafID, buf, err := t.descend(key)
	if err != nil {
		return nil, false, err
	}
	defer t.pool.Unpin(leafID, false)

	idx, found, err := SearchLeaf(buf, key)
	if err != nil || !found {
		return nil, false, err
	}
	c, err := ReadLeafCell(buf, idx)
	if err != nil {
		return nil, false, err
	}
	v, ok := VisibleVersion(c, snap)
	if !ok {
		return nil, false, nil
	}
	if v.OverflowPage != 0 {
		val, err := t.readOverflow(v.OverflowPage, int(v.ValueLen))
		return val, err == nil, err
	}
	return v.Value, true, nil
}

// Put sets key to value at sequence. Versions no snapshot at or after horizon
// can see are dropped. No-op if sequence is not newer than the key's head
// (WAL replay safety).
func (t *Tree) Put(key, value []byte, sequence, horizon uint64) error {
	v := append([]byte(nil), value...)
	return t.apply(key, Version{Sequence: sequence, Value: v, ValueLen: uint32(len(v))}, horizon)
}

// Delete adds a tombstone at sequence (see Put for horizon).
func (t *Tree) Delete(key []byte, sequence, horizon uint64) error {
	return t.apply(key, Version{Sequence: sequence, Deleted: true}, horizon)
}

func (t *Tree) apply(key []byte, ver Version, horizon uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	path, leafID, buf, err := t.descend(key)
	if err != nil {
		return err
	}
	leafDirty := false
	defer func() {
		if leafID != 0 {
			t.pool.Unpin(leafID, leafDirty)
		}
	}()

	idx, found, err := SearchLeaf(buf, key)
	if err != nil {
		return err
	}
	var cell Cell
	if found {
		if cell, err = ReadLeafCell(buf, idx); err != nil {
			return err
		}
		if len(cell.Versions) > 0 && cell.Versions[0].Sequence >= ver.Sequence {
			return nil
		}
	} else {
		if ver.Deleted {
			return nil
		}
		cell = Cell{Key: append([]byte(nil), key...)}
	}
	cell.Versions = append([]Version{ver}, cell.Versions...)

	var created []uint32
	fail := func(err error) error {
		for _, head := range created {
			_ = t.freeOverflow(head)
		}
		return err
	}
	dropped := pruneVersions(&cell, horizon)
	if created, err = t.shrinkCell(&cell); err != nil {
		return fail(err)
	}

	// From here the tree is modified; no flush may happen until promote returns.
	if err := t.reserve(2*(len(path)+1) + 2); err != nil {
		return fail(err)
	}
	cells, err := cellsFromLeaf(buf)
	if err != nil {
		return fail(err)
	}
	cells = spliceCell(cells, idx, cell, found)
	if leafFits(cells) {
		if err := writeLeaf(buf, cells, RightSibling(buf)); err != nil {
			return fail(err)
		}
		leafDirty = true
	} else {
		sep, rightID, err := t.splitLeaf(buf, cells)
		if err != nil {
			return fail(err)
		}
		leafDirty = true
		t.pool.Unpin(leafID, true)
		leafID = 0
		if err := t.promote(path, sep, rightID); err != nil {
			return err
		}
	}
	if leafID != 0 {
		t.pool.Unpin(leafID, leafDirty)
		leafID = 0
	}
	for _, head := range dropped {
		if err := t.freeOverflow(head); err != nil {
			return err
		}
	}
	return nil
}

// pruneVersions keeps versions newer than horizon plus the one visible at
// horizon; it returns overflow chains of dropped versions.
func pruneVersions(c *Cell, horizon uint64) (dropped []uint32) {
	for j, v := range c.Versions {
		if v.Sequence > horizon {
			continue
		}
		for _, old := range c.Versions[j+1:] {
			if old.OverflowPage != 0 {
				dropped = append(dropped, old.OverflowPage)
			}
		}
		c.Versions = c.Versions[:j+1]
		return dropped
	}
	return nil
}

// shrinkCell moves inline values to overflow pages, oldest first, until the
// cell fits in half a page. Returns the overflow chains it created.
func (t *Tree) shrinkCell(c *Cell) ([]uint32, error) {
	var created []uint32
	for i := len(c.Versions) - 1; i >= 0 && cellFootprint(*c) > maxCellFootprint; i-- {
		v := &c.Versions[i]
		if v.Deleted || v.OverflowPage != 0 || len(v.Value) == 0 {
			continue
		}
		head, err := t.writeOverflow(v.Value)
		if err != nil {
			return created, err
		}
		created = append(created, head)
		v.OverflowPage, v.ValueLen, v.Value = head, uint32(len(v.Value)), nil
	}
	if cellFootprint(*c) > maxCellFootprint {
		return created, ErrTooBig
	}
	return created, nil
}

func (t *Tree) writeOverflow(val []byte) (uint32, error) {
	var next uint32
	for end := len(val); end > 0; {
		start := end - page.OverflowCapacity
		if start < 0 {
			start = 0
		}
		if err := t.reserve(1); err != nil {
			return 0, err
		}
		id, buf, err := t.pool.NewPage(page.InitOverflow)
		if err != nil {
			return 0, err
		}
		if err := page.WriteOverflowChunk(buf, next, val[start:end]); err != nil {
			t.pool.Unpin(id, true)
			return 0, err
		}
		t.pool.Unpin(id, true)
		next = id
		end = start
	}
	return next, nil
}

func (t *Tree) readOverflow(head uint32, total int) ([]byte, error) {
	out := make([]byte, 0, total)
	for id := head; id != 0; {
		buf, err := t.pool.Get(id)
		if err != nil {
			return nil, err
		}
		next, chunk, err := page.ReadOverflowChunk(buf)
		t.pool.Unpin(id, false)
		if err != nil {
			return nil, err
		}
		out = append(out, chunk...)
		id = next
	}
	return out, nil
}

func (t *Tree) freeOverflow(head uint32) error {
	for id := head; id != 0; {
		if err := t.reserve(1); err != nil {
			return err
		}
		buf, err := t.pool.Get(id)
		if err != nil {
			return err
		}
		next, _, err := page.ReadOverflowChunk(buf)
		t.pool.Unpin(id, false)
		if err != nil {
			return err
		}
		if err := t.pool.FreePage(id); err != nil {
			return err
		}
		id = next
	}
	return nil
}

func (t *Tree) leftmostLeaf() (uint32, error) {
	id := t.root
	for {
		buf, err := t.pool.Get(id)
		if err != nil {
			return 0, err
		}
		typ, child := TypeOf(buf), LeftChild(buf)
		t.pool.Unpin(id, false)
		switch typ {
		case page.TypeBTreeLeaf:
			return id, nil
		case page.TypeBTreeInternal:
			id = child
		default:
			return 0, ErrBadType
		}
	}
}

func (t *Tree) forEachLeafCell(fn func(Cell)) error {
	id, err := t.leftmostLeaf()
	if err != nil {
		return err
	}
	for id != 0 {
		buf, err := t.pool.Get(id)
		if err != nil {
			return err
		}
		cells, err := cellsFromLeaf(buf)
		next := RightSibling(buf)
		t.pool.Unpin(id, false)
		if err != nil {
			return err
		}
		for _, c := range cells {
			fn(c)
		}
		id = next
	}
	return nil
}

// LiveCount counts keys whose newest version is not a tombstone.
func (t *Tree) LiveCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n := 0
	_ = t.forEachLeafCell(func(c Cell) {
		if len(c.Versions) > 0 && !c.Versions[0].Deleted {
			n++
		}
	})
	return n
}

// ApproxLiveBytes sums key + newest value sizes of live keys.
func (t *Tree) ApproxLiveBytes() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var sum int64
	_ = t.forEachLeafCell(func(c Cell) {
		if len(c.Versions) > 0 && !c.Versions[0].Deleted {
			sum += int64(len(c.Key)) + int64(c.Versions[0].ValueLen)
		}
	})
	return sum
}

// Prune drops versions no snapshot at or after horizon can see, and with
// forgetDeleted also drops keys whose surviving newest version is a tombstone.
func (t *Tree) Prune(horizon uint64, forgetDeleted bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	id, err := t.leftmostLeaf()
	if err != nil {
		return err
	}
	for id != 0 {
		if err := t.reserve(2); err != nil {
			return err
		}
		buf, err := t.pool.Get(id)
		if err != nil {
			return err
		}
		next := RightSibling(buf)
		cells, err := cellsFromLeaf(buf)
		if err != nil {
			t.pool.Unpin(id, false)
			return err
		}
		var dropped []uint32
		changed := false
		out := cells[:0]
		for _, c := range cells {
			before := len(c.Versions)
			dropped = append(dropped, pruneVersions(&c, horizon)...)
			if len(c.Versions) != before {
				changed = true
			}
			if forgetDeleted && c.Versions[0].Deleted && c.Versions[0].Sequence <= horizon {
				dropped = append(dropped, overflowHeads(c)...)
				changed = true
				continue
			}
			out = append(out, c)
		}
		if changed {
			if err := writeLeaf(buf, out, next); err != nil {
				t.pool.Unpin(id, false)
				return err
			}
		}
		t.pool.Unpin(id, changed)
		for _, head := range dropped {
			if err := t.freeOverflow(head); err != nil {
				return err
			}
		}
		id = next
	}
	return nil
}

func overflowHeads(c Cell) []uint32 {
	var out []uint32
	for _, v := range c.Versions {
		if v.OverflowPage != 0 {
			out = append(out, v.OverflowPage)
		}
	}
	return out
}

// Iterator walks keys in order. It holds the tree's read lock until Close,
// so do not write to the tree from the goroutine that holds an open Iterator.
type Iterator struct {
	t      *Tree
	pageID uint32
	buf    []byte
	idx    int
	err    error
	closed bool
}

// Seek positions at the first key >= key.
func (t *Tree) Seek(key []byte) (*Iterator, error) {
	t.mu.RLock()
	_, id, buf, err := t.descend(key)
	if err != nil {
		t.mu.RUnlock()
		return nil, err
	}
	idx, _, err := SearchLeaf(buf, key)
	if err != nil {
		t.pool.Unpin(id, false)
		t.mu.RUnlock()
		return nil, err
	}
	it := &Iterator{t: t, pageID: id, buf: buf, idx: idx}
	it.settle()
	return it, it.err
}

// settle skips past exhausted (possibly empty) leaves.
func (it *Iterator) settle() {
	for it.pageID != 0 && it.idx >= NKeys(it.buf) {
		next := RightSibling(it.buf)
		it.t.pool.Unpin(it.pageID, false)
		it.pageID, it.buf, it.idx = 0, nil, 0
		if next == 0 {
			return
		}
		buf, err := it.t.pool.Get(next)
		if err != nil {
			it.err = err
			return
		}
		it.pageID, it.buf = next, buf
	}
}

func (it *Iterator) Valid() bool { return it.pageID != 0 && it.err == nil }

func (it *Iterator) Err() error { return it.err }

// Key returns a copy of the current key.
func (it *Iterator) Key() ([]byte, error) {
	k, err := keyAt(it.buf, it.idx)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), k...), nil
}

// Cell returns the current key with its full version chain.
func (it *Iterator) Cell() (Cell, error) { return ReadLeafCell(it.buf, it.idx) }

func (it *Iterator) Next() error {
	if !it.Valid() {
		return it.err
	}
	it.idx++
	it.settle()
	return it.err
}

func (it *Iterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	if it.pageID != 0 {
		it.t.pool.Unpin(it.pageID, false)
		it.pageID, it.buf = 0, nil
	}
	it.t.mu.RUnlock()
}
