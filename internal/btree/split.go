package btree

import "bytes"

// splitLeaf writes cells (which no longer fit in one page) across left and a
// new right leaf, split by bytes. Returns the separator (smallest key on the
// right) and the right page id. left stays pinned; the caller unpins it dirty.
func (t *Tree) splitLeaf(left []byte, cells []Cell) (sepKey []byte, rightID uint32, err error) {
	sizes := make([]int, len(cells))
	for i, c := range cells {
		sizes[i] = cellFootprint(c)
	}
	k, ok := splitPoint(sizes, 0)
	if !ok {
		return nil, 0, ErrTooBig
	}
	rightID, right, err := t.pool.NewPage(InitLeaf)
	if err != nil {
		return nil, 0, err
	}
	if err := writeLeaf(right, cells[k:], RightSibling(left)); err != nil {
		t.pool.Unpin(rightID, true)
		return nil, 0, err
	}
	t.pool.Unpin(rightID, true)
	if err := writeLeaf(left, cells[:k], rightID); err != nil {
		return nil, 0, err
	}
	return append([]byte(nil), cells[k].Key...), rightID, nil
}

// promote inserts (sepKey → rightChild) into the parents on path (root→parent),
// splitting internals as needed and growing a new root at the top.
func (t *Tree) promote(path []uint32, sepKey []byte, rightChild uint32) error {
	for len(path) > 0 {
		parentID := path[len(path)-1]
		path = path[:len(path)-1]
		buf, err := t.pool.Get(parentID)
		if err != nil {
			return err
		}
		left, seps, err := sepsFromInternal(buf)
		if err != nil {
			t.pool.Unpin(parentID, false)
			return err
		}
		ins := len(seps)
		for i, s := range seps {
			if bytes.Compare(sepKey, s.Key) < 0 {
				ins = i
				break
			}
		}
		seps = append(seps[:ins], append([]sepEntry{{Key: sepKey, Child: rightChild}}, seps[ins:]...)...)

		if internalFits(seps) {
			err := writeInternal(buf, left, seps)
			t.pool.Unpin(parentID, err == nil)
			return err
		}
		sepKey, rightChild, err = t.splitInternal(buf, left, seps)
		t.pool.Unpin(parentID, err == nil)
		if err != nil {
			return err
		}
	}

	rootID, rootBuf, err := t.pool.NewPage(InitInternal)
	if err != nil {
		return err
	}
	err = writeInternal(rootBuf, t.root, []sepEntry{{Key: sepKey, Child: rightChild}})
	t.pool.Unpin(rootID, true)
	if err != nil {
		return err
	}
	t.root = rootID
	return t.persistRoot()
}

// splitInternal keeps seps[:k] in buf, moves seps[k+1:] to a new right page
// (whose leftChild is seps[k].Child) and returns seps[k].Key to push up.
func (t *Tree) splitInternal(buf []byte, leftChild uint32, seps []sepEntry) (sepKey []byte, rightID uint32, err error) {
	sizes := make([]int, len(seps))
	for i, s := range seps {
		sizes[i] = sepFootprint(s.Key)
	}
	k, ok := splitPoint(sizes, 1)
	if !ok {
		return nil, 0, ErrTooBig
	}
	rightID, right, err := t.pool.NewPage(InitInternal)
	if err != nil {
		return nil, 0, err
	}
	err = writeInternal(right, seps[k].Child, seps[k+1:])
	t.pool.Unpin(rightID, true)
	if err != nil {
		return nil, 0, err
	}
	if err := writeInternal(buf, leftChild, seps[:k]); err != nil {
		return nil, 0, err
	}
	return seps[k].Key, rightID, nil
}
