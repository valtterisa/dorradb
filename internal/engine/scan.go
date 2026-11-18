package engine

import "dorradb/internal/btree"

// Scanner walks visible key/value pairs at a snapshot.
type Scanner struct {
	it   *btree.Iterator
	snap uint64
	err  error
	key  []byte
	val  []byte
	ok   bool
}

// ScanAt returns an iterator over keys >= startKey in table (empty = default tree)
// visible at snapshotSequence. Caller must Close the scanner.
func (e *Engine) ScanAt(table string, snapshotSequence uint64, startKey []byte) (*Scanner, error) {
	tr, err := e.tableTree(table)
	if err != nil {
		return nil, err
	}
	if startKey == nil {
		startKey = []byte{}
	}
	it, err := tr.Seek(startKey)
	if err != nil {
		return nil, err
	}
	s := &Scanner{it: it, snap: snapshotSequence}
	s.advance()
	return s, s.err
}

func (s *Scanner) advance() {
	s.ok = false
	s.key, s.val = nil, nil
	for s.it.Valid() {
		k, err := s.it.Key()
		if err != nil {
			s.err = err
			return
		}
		v, visible, err := s.it.VisibleValue(s.snap)
		if err != nil {
			s.err = err
			return
		}
		if err := s.it.Next(); err != nil {
			s.err = err
			return
		}
		if !visible {
			continue
		}
		s.key = k
		s.val = v
		s.ok = true
		return
	}
	s.err = s.it.Err()
}

func (s *Scanner) Valid() bool { return s.ok && s.err == nil }

func (s *Scanner) Err() error { return s.err }

func (s *Scanner) Key() []byte { return s.key }

func (s *Scanner) Value() []byte { return s.val }

func (s *Scanner) Next() {
	if !s.ok {
		return
	}
	s.advance()
}

func (s *Scanner) Close() {
	if s.it != nil {
		s.it.Close()
		s.it = nil
	}
	s.ok = false
}
