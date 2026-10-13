package storage

import (
	"dorradb/internal/encode"
	"dorradb/internal/index"
	"dorradb/internal/storage/page"
	"dorradb/internal/vfs"
)

// Store is the heap access method on top of the pager.
type Store struct {
	pager      *Pager
	insertHint uint32 // last heap page that accepted an insert
}

// OpenStore opens the paged main file.
func OpenStore(fs vfs.FS, dbPath string, create bool) (*Store, error) {
	p, err := Open(fs, dbPath, create)
	if err != nil {
		return nil, err
	}
	return &Store{pager: p}, nil
}

func (s *Store) Path() string                    { return s.pager.Path() }
func (s *Store) Size() int64                     { return s.pager.Size() }
func (s *Store) Header() encode.Header           { return s.pager.Header() }
func (s *Store) SetHeader(h encode.Header) error { return s.pager.SetHeader(h) }
func (s *Store) FlushAll() error                 { return s.pager.FlushAll() }
func (s *Store) Close() error                    { return s.pager.Close() }

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
		id, buf, err := s.pager.NewPage(page.InitOverflow)
		if err != nil {
			return 0, err
		}
		if err := page.WriteOverflowChunk(buf, next, val[start:off]); err != nil {
			s.pager.Unpin(id, false)
			return 0, err
		}
		s.pager.Unpin(id, true)
		next = id
		head = id
		off = start
	}
	return head, nil
}

func (s *Store) insertHeap(r page.Record) (pageID uint32, slot uint16, nbytes int, err error) {
	nbytes = page.RecordSize(r) + 2

	try := func(id uint32) (uint16, bool, error) {
		buf, err := s.pager.Get(id)
		if err != nil {
			return 0, false, err
		}
		if page.TypeOf(buf) != page.TypeHeap {
			s.pager.Unpin(id, false)
			return 0, false, nil
		}
		sl, err := page.Insert(buf, r)
		if err == page.ErrPageFull {
			s.pager.Unpin(id, false)
			return 0, false, nil
		}
		if err != nil {
			s.pager.Unpin(id, false)
			return 0, false, err
		}
		s.pager.Unpin(id, true)
		return sl, true, nil
	}

	if s.insertHint != 0 {
		if sl, ok, err := try(s.insertHint); err != nil {
			return 0, 0, 0, err
		} else if ok {
			return s.insertHint, sl, nbytes, nil
		}
	}

	// No full-file scan: allocate a new heap page when the hint is full.
	id, buf, err := s.pager.NewPage(page.InitHeap)
	if err != nil {
		return 0, 0, 0, err
	}
	sl, err := page.Insert(buf, r)
	if err != nil {
		s.pager.Unpin(id, false)
		return 0, 0, 0, err
	}
	s.pager.Unpin(id, true)
	s.insertHint = id
	return id, sl, nbytes, nil
}

// ReadValue loads value bytes for loc (follows overflow).
func (s *Store) ReadValue(loc index.Loc) ([]byte, error) {
	if loc.Deleted {
		return nil, nil
	}
	buf, err := s.pager.Get(loc.PageID)
	if err != nil {
		return nil, err
	}
	rec, _, err := page.ReadSlot(buf, loc.Slot)
	s.pager.Unpin(loc.PageID, false)
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
		buf, err := s.pager.Get(id)
		if err != nil {
			return nil, err
		}
		next, chunk, err := page.ReadOverflowChunk(buf)
		s.pager.Unpin(id, false)
		if err != nil {
			return nil, err
		}
		out = append(out, chunk...)
		id = next
	}
	return out, nil
}

// ScanIndex rebuilds the RAM index from all heap page slots.
func (s *Store) ScanIndex(idx *index.Index) error {
	idx.Reset()
	for id := uint32(1); id < s.pager.hdr.PageCount; id++ {
		buf, err := s.pager.Get(id)
		if err != nil {
			return err
		}
		if page.TypeOf(buf) != page.TypeHeap {
			s.pager.Unpin(id, false)
			continue
		}
		n := page.NSlots(buf)
		for slot := uint16(0); slot < n; slot++ {
			rec, nbytes, err := page.ReadSlot(buf, slot)
			if err != nil {
				s.pager.Unpin(id, false)
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
		s.pager.Unpin(id, false)
	}
	return nil
}
