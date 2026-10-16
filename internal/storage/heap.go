package storage

import (
	"dorradb/internal/encode"
	"dorradb/internal/index"
	"dorradb/internal/storage/page"
	"dorradb/internal/vfs"
)

// Store is the heap access method on top of the buffer pool.
type Store struct {
	pool       *BufferPool
	insertHint uint32         // last heap page that accepted an insert
	freeBytes  map[uint32]int // FSM: heap pageID → FreeSpace (rebuild on open)
}

// OpenStore opens the paged main file.
func OpenStore(fs vfs.FS, dbPath string, create bool, opts OpenOptions) (*Store, error) {
	p, err := Open(fs, dbPath, create, opts)
	if err != nil {
		return nil, err
	}
	return &Store{
		pool:      p,
		freeBytes: make(map[uint32]int),
	}, nil
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

// Append writes a logical record onto heap pages (overflow if needed).
func (s *Store) Append(rec encode.Record) (index.Loc, error) {
	r := page.Record{
		Sequence: rec.Sequence,
		Op:       rec.Op,
		Key:      rec.Key,
		Value:    rec.Value,
		ValueLen: uint32(len(rec.Value)),
	}
	var overflowHead uint32
	maxPayload := page.Size - page.HeapHdrSize - 2
	if page.RecordSize(r) > maxPayload && len(rec.Value) > 0 {
		head, err := s.writeOverflow(rec.Value)
		if err != nil {
			return index.Loc{}, err
		}
		overflowHead = head
		r.Value = nil
		r.OverflowPage = head
		r.ValueLen = uint32(len(rec.Value))
	}

	pageID, slot, nbytes, err := s.insertHeap(r)
	if err != nil {
		return index.Loc{}, err
	}
	return index.Loc{
		PageID:       pageID,
		Slot:         slot,
		OverflowPage: overflowHead,
		Bytes:        nbytes,
		Sequence:     rec.Sequence,
		Deleted:      rec.Op == encode.OpDelete,
	}, nil
}

func (s *Store) writeOverflow(val []byte) (uint32, error) {
	// Write last chunk first so next pointers chain forward from the head.
	var next, head uint32
	for off := len(val); off > 0; {
		n := page.OverflowCapacity
		if off < n {
			n = off
		}
		start := off - n
		id, buf, err := s.pool.NewPage(page.InitOverflow)
		if err != nil {
			return 0, err
		}
		if err := page.WriteOverflowChunk(buf, next, val[start:off]); err != nil {
			s.pool.Unpin(id, false)
			return 0, err
		}
		s.pool.Unpin(id, true)
		next = id
		head = id
		off = start
	}
	return head, nil
}

func (s *Store) insertHeap(r page.Record) (pageID uint32, slot uint16, nbytes int, err error) {
	nbytes = page.RecordSize(r) + 2
	need := page.RecordSize(r)

	try := func(id uint32) (uint16, bool, error) {
		buf, err := s.pool.Get(id)
		if err != nil {
			return 0, false, err
		}
		if page.TypeOf(buf) != page.TypeHeap {
			s.pool.Unpin(id, false)
			delete(s.freeBytes, id)
			return 0, false, nil
		}
		sl, err := page.Insert(buf, r)
		if err == page.ErrPageFull {
			s.freeBytes[id] = page.FreeSpace(buf)
			s.pool.Unpin(id, false)
			return 0, false, nil
		}
		if err != nil {
			s.pool.Unpin(id, false)
			return 0, false, err
		}
		s.freeBytes[id] = page.FreeSpace(buf)
		s.pool.Unpin(id, true)
		return sl, true, nil
	}

	if s.insertHint != 0 && s.freeBytes[s.insertHint] >= need {
		if sl, ok, err := try(s.insertHint); err != nil {
			return 0, 0, 0, err
		} else if ok {
			return s.insertHint, sl, nbytes, nil
		}
	}

	// First-fit over FSM for leftover free space on older pages.
	for id, free := range s.freeBytes {
		if id == s.insertHint || free < need {
			continue
		}
		if sl, ok, err := try(id); err != nil {
			return 0, 0, 0, err
		} else if ok {
			s.insertHint = id
			return id, sl, nbytes, nil
		}
	}

	id, buf, err := s.pool.NewPage(page.InitHeap)
	if err != nil {
		return 0, 0, 0, err
	}
	sl, err := page.Insert(buf, r)
	if err != nil {
		s.pool.Unpin(id, false)
		return 0, 0, 0, err
	}
	s.freeBytes[id] = page.FreeSpace(buf)
	s.pool.Unpin(id, true)
	s.insertHint = id
	return id, sl, nbytes, nil
}

// ReadValue loads value bytes for loc (follows overflow).
func (s *Store) ReadValue(loc index.Loc) ([]byte, error) {
	if loc.Deleted {
		return nil, nil
	}
	buf, err := s.pool.Get(loc.PageID)
	if err != nil {
		return nil, err
	}
	rec, _, err := page.ReadSlot(buf, loc.Slot)
	s.pool.Unpin(loc.PageID, false)
	if err != nil {
		return nil, err
	}
	if rec.OverflowPage == 0 {
		return append([]byte(nil), rec.Value...), nil
	}
	return s.readOverflow(rec.OverflowPage, int(rec.ValueLen))
}

func (s *Store) readOverflow(head uint32, total int) ([]byte, error) {
	out := make([]byte, 0, total)
	for id := head; id != 0; {
		buf, err := s.pool.Get(id)
		if err != nil {
			return nil, err
		}
		next, chunk, err := page.ReadOverflowChunk(buf)
		s.pool.Unpin(id, false)
		if err != nil {
			return nil, err
		}
		out = append(out, chunk...)
		id = next
	}
	return out, nil
}

// ScanIndex rebuilds the RAM index and free-space map from all heap page slots.
func (s *Store) ScanIndex(idx *index.Index) error {
	idx.Reset()
	s.freeBytes = make(map[uint32]int)
	s.insertHint = 0
	for id := uint32(1); id < s.pool.hdr.PageCount; id++ {
		buf, err := s.pool.Get(id)
		if err != nil {
			return err
		}
		if page.TypeOf(buf) != page.TypeHeap {
			s.pool.Unpin(id, false)
			continue
		}
		s.freeBytes[id] = page.FreeSpace(buf)
		s.insertHint = id
		n := page.NSlots(buf)
		for slot := uint16(0); slot < n; slot++ {
			rec, nbytes, err := page.ReadSlot(buf, slot)
			if err != nil {
				s.pool.Unpin(id, false)
				return err
			}
			idx.Apply(rec.Key, index.Loc{
				PageID:       id,
				Slot:         slot,
				OverflowPage: rec.OverflowPage,
				Bytes:        nbytes + 2,
				Sequence:     rec.Sequence,
				Deleted:      rec.Op == encode.OpDelete,
			})
		}
		s.pool.Unpin(id, false)
	}
	return nil
}

const vacuumDeadSlotRatio = 0.25

// Vacuum compacts heap pages and frees unreferenced overflow/heap pages.
// Caller must hold no reader pins and should have pruned the index first.
func (s *Store) Vacuum(idx *index.Index) error {
	idx.ForgetDeleted()
	refs := idx.ReferencedSlots()

	// Collect overflow pages still referenced by live Locs.
	liveOverflow := make(map[uint32]struct{})
	for pageID, slots := range refs {
		buf, err := s.pool.Get(pageID)
		if err != nil {
			return err
		}
		if page.TypeOf(buf) != page.TypeHeap {
			s.pool.Unpin(pageID, false)
			continue
		}
		for slot := range slots {
			rec, _, err := page.ReadSlot(buf, slot)
			if err != nil {
				s.pool.Unpin(pageID, false)
				return err
			}
			if err := s.walkOverflow(rec.OverflowPage, func(oid uint32) {
				liveOverflow[oid] = struct{}{}
			}); err != nil {
				s.pool.Unpin(pageID, false)
				return err
			}
		}
		s.pool.Unpin(pageID, false)
	}

	for id := uint32(1); id < s.pool.hdr.PageCount; id++ {
		buf, err := s.pool.Get(id)
		if err != nil {
			return err
		}
		typ := page.TypeOf(buf)
		if typ == page.TypeOverflow {
			s.pool.Unpin(id, false)
			if _, keep := liveOverflow[id]; !keep {
				// Only free if not already on freelist (type cleared).
				if err := s.pool.FreePage(id); err != nil {
					return err
				}
			}
			continue
		}
		if typ != page.TypeHeap {
			s.pool.Unpin(id, false)
			continue
		}

		n := page.NSlots(buf)
		live := refs[id]
		deadSlots := 0
		for slot := uint16(0); slot < n; slot++ {
			if _, ok := live[slot]; !ok {
				deadSlots++
			}
		}

		if n > 0 && deadSlots == int(n) {
			// Free overflow for every slot, then the heap page.
			for slot := uint16(0); slot < n; slot++ {
				rec, _, err := page.ReadSlot(buf, slot)
				if err != nil {
					s.pool.Unpin(id, false)
					return err
				}
				if err := s.freeOverflowChain(rec.OverflowPage, liveOverflow); err != nil {
					s.pool.Unpin(id, false)
					return err
				}
			}
			s.pool.Unpin(id, false)
			delete(s.freeBytes, id)
			if s.insertHint == id {
				s.insertHint = 0
			}
			if err := s.pool.FreePage(id); err != nil {
				return err
			}
			continue
		}

		if n == 0 || deadSlots == 0 || float64(deadSlots)/float64(n) < vacuumDeadSlotRatio {
			s.freeBytes[id] = page.FreeSpace(buf)
			s.pool.Unpin(id, false)
			continue
		}

		// Compact: rewrite live slots only; free dead overflow chains.
		slotMap := make(map[uint16]uint16)
		tmp := make([]byte, page.Size)
		page.InitHeap(tmp)
		var newSlot uint16
		for slot := uint16(0); slot < n; slot++ {
			rec, _, err := page.ReadSlot(buf, slot)
			if err != nil {
				s.pool.Unpin(id, false)
				return err
			}
			if _, ok := live[slot]; !ok {
				if err := s.freeOverflowChain(rec.OverflowPage, liveOverflow); err != nil {
					s.pool.Unpin(id, false)
					return err
				}
				continue
			}
			ns, err := page.Insert(tmp, rec)
			if err != nil {
				s.pool.Unpin(id, false)
				return err
			}
			slotMap[slot] = ns
			_ = newSlot
		}
		copy(buf, tmp)
		s.pool.MarkDirty(id)
		s.freeBytes[id] = page.FreeSpace(buf)
		s.pool.Unpin(id, true)
		idx.RemapSlots(id, slotMap)
	}

	// Refresh FSM for remaining heap pages / drop freelist entries.
	s.rebuildFSM()
	return nil
}

func (s *Store) rebuildFSM() {
	s.freeBytes = make(map[uint32]int)
	s.insertHint = 0
	for id := uint32(1); id < s.pool.hdr.PageCount; id++ {
		buf, err := s.pool.Get(id)
		if err != nil {
			continue
		}
		if page.TypeOf(buf) == page.TypeHeap {
			s.freeBytes[id] = page.FreeSpace(buf)
			s.insertHint = id
		}
		s.pool.Unpin(id, false)
	}
}

func (s *Store) walkOverflow(head uint32, fn func(uint32)) error {
	for id := head; id != 0; {
		fn(id)
		buf, err := s.pool.Get(id)
		if err != nil {
			return err
		}
		next, _, err := page.ReadOverflowChunk(buf)
		s.pool.Unpin(id, false)
		if err != nil {
			return err
		}
		id = next
	}
	return nil
}

func (s *Store) freeOverflowChain(head uint32, live map[uint32]struct{}) error {
	for id := head; id != 0; {
		if _, keep := live[id]; keep {
			return nil
		}
		buf, err := s.pool.Get(id)
		if err != nil {
			return err
		}
		next, _, err := page.ReadOverflowChunk(buf)
		s.pool.Unpin(id, false)
		if err != nil {
			// Already freed / not overflow — stop.
			return nil
		}
		if err := s.pool.FreePage(id); err != nil {
			return err
		}
		id = next
	}
	return nil
}
