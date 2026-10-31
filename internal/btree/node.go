// Package btree is a clustered table B+ tree on the buffer pool.
// Leaf cells hold user key + packed MVCC version chain (value inline or overflow).
package btree

import (
	"bytes"
	"encoding/binary"
	"errors"

	"dorradb/internal/storage/page"
)

const (
	hdrSize = 16

	hdrType         = 0
	hdrNKeys        = 2  // u16
	hdrLower        = 4  // u16 — end of cell payload
	hdrUpper        = 6  // u16 — start of slot directory
	hdrRightSibling = 8  // u32 (leaves)
	hdrLeftChild    = 12 // u32 (internals: child for keys < first sep)

	slotBytes = 2

	// pageCapacity is the bytes available for cells + slots on one page.
	pageCapacity = page.Size - hdrSize
	// maxCellFootprint caps one cell (+ its slot) at half a page, so any
	// overfull page can always be split into two halves that both fit.
	maxCellFootprint = pageCapacity / 2

	flagOverflow uint8 = 1
	flagDeleted  uint8 = 2
)

var (
	ErrPageFull = errors.New("btree: page full")
	ErrCorrupt  = errors.New("btree: corrupt")
	ErrBadType  = errors.New("btree: bad page type")
	ErrTooBig   = errors.New("btree: cell too large (too many live versions)")
)

// Version is one MVCC stamp on a key (newest-first in cells).
type Version struct {
	Sequence     uint64
	Deleted      bool
	Value        []byte // inline; nil when OverflowPage != 0 or deleted
	OverflowPage uint32 // 0 = inline / empty
	ValueLen     uint32
}

// Cell is one leaf entry: user key + version chain.
type Cell struct {
	Key      []byte
	Versions []Version
}

type sepEntry struct {
	Key   []byte
	Child uint32 // child for keys >= Key
}

func InitLeaf(buf []byte) {
	initNode(buf, page.TypeBTreeLeaf)
}

func InitInternal(buf []byte) {
	initNode(buf, page.TypeBTreeInternal)
}

func initNode(buf []byte, typ uint8) {
	if len(buf) < page.Size {
		panic("btree: short page")
	}
	clear(buf)
	buf[hdrType] = typ
	binary.LittleEndian.PutUint16(buf[hdrLower:], hdrSize)
	binary.LittleEndian.PutUint16(buf[hdrUpper:], page.Size)
}

func TypeOf(buf []byte) uint8 {
	if len(buf) < 1 {
		return 0
	}
	return buf[hdrType]
}

func NKeys(buf []byte) int {
	return int(binary.LittleEndian.Uint16(buf[hdrNKeys:]))
}

func RightSibling(buf []byte) uint32 {
	return binary.LittleEndian.Uint32(buf[hdrRightSibling:])
}

func SetRightSibling(buf []byte, id uint32) {
	binary.LittleEndian.PutUint32(buf[hdrRightSibling:], id)
}

func LeftChild(buf []byte) uint32 {
	return binary.LittleEndian.Uint32(buf[hdrLeftChild:])
}

func SetLeftChild(buf []byte, id uint32) {
	binary.LittleEndian.PutUint32(buf[hdrLeftChild:], id)
}

func cellOffset(buf []byte, i int) (int, error) {
	n := NKeys(buf)
	if i < 0 || i >= n {
		return 0, ErrCorrupt
	}
	// Directory grows downward; first appended cell is slot 0 at the high end.
	upper := int(binary.LittleEndian.Uint16(buf[hdrUpper:]))
	pos := upper + (n-1-i)*slotBytes
	if pos+slotBytes > page.Size {
		return 0, ErrCorrupt
	}
	off := int(binary.LittleEndian.Uint16(buf[pos:]))
	if off < hdrSize || off >= upper {
		return 0, ErrCorrupt
	}
	return off, nil
}

// keyAt returns the key of entry i (leaf or internal) without decoding values.
// The slice aliases the page; copy it before the page is unpinned.
func keyAt(buf []byte, i int) ([]byte, error) {
	off, err := cellOffset(buf, i)
	if err != nil {
		return nil, err
	}
	keyLen := int(binary.LittleEndian.Uint16(buf[off:]))
	if off+2+keyLen > page.Size {
		return nil, ErrCorrupt
	}
	return buf[off+2 : off+2+keyLen], nil
}

// appendEntry writes enc as entry n and extends the slot directory.
func appendEntry(buf []byte, enc []byte) error {
	lower := int(binary.LittleEndian.Uint16(buf[hdrLower:]))
	upper := int(binary.LittleEndian.Uint16(buf[hdrUpper:]))
	if upper-lower < len(enc)+slotBytes {
		return ErrPageFull
	}
	n := NKeys(buf)
	copy(buf[lower:], enc)
	newUpper := upper - slotBytes
	binary.LittleEndian.PutUint16(buf[newUpper:], uint16(lower))
	binary.LittleEndian.PutUint16(buf[hdrLower:], uint16(lower+len(enc)))
	binary.LittleEndian.PutUint16(buf[hdrUpper:], uint16(newUpper))
	binary.LittleEndian.PutUint16(buf[hdrNKeys:], uint16(n+1))
	return nil
}

// --- leaf cells ---

func cellSize(c Cell) int {
	n := 2 + len(c.Key) + 2
	for _, v := range c.Versions {
		n += 8 + 1 + 4
		switch {
		case v.Deleted:
		case v.OverflowPage != 0:
			n += 4
		default:
			n += len(v.Value)
		}
	}
	return n
}

func cellFootprint(c Cell) int { return cellSize(c) + slotBytes }

func encodeCell(c Cell) []byte {
	dst := make([]byte, 0, cellSize(c))
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(c.Key)))
	dst = append(dst, c.Key...)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(c.Versions)))
	for _, v := range c.Versions {
		dst = binary.LittleEndian.AppendUint64(dst, v.Sequence)
		var fl uint8
		switch {
		case v.Deleted:
			fl = flagDeleted
		case v.OverflowPage != 0:
			fl = flagOverflow
		}
		dst = append(dst, fl)
		switch {
		case v.Deleted:
			dst = binary.LittleEndian.AppendUint32(dst, 0)
		case v.OverflowPage != 0:
			dst = binary.LittleEndian.AppendUint32(dst, v.ValueLen)
			dst = binary.LittleEndian.AppendUint32(dst, v.OverflowPage)
		default:
			dst = binary.LittleEndian.AppendUint32(dst, uint32(len(v.Value)))
			dst = append(dst, v.Value...)
		}
	}
	return dst
}

func decodeCell(buf []byte, off int) (Cell, error) {
	if off+2 > page.Size {
		return Cell{}, ErrCorrupt
	}
	keyLen := int(binary.LittleEndian.Uint16(buf[off:]))
	off += 2
	if off+keyLen+2 > page.Size {
		return Cell{}, ErrCorrupt
	}
	key := append([]byte(nil), buf[off:off+keyLen]...)
	off += keyLen
	nVers := int(binary.LittleEndian.Uint16(buf[off:]))
	off += 2
	vers := make([]Version, 0, nVers)
	for i := 0; i < nVers; i++ {
		if off+8+1+4 > page.Size {
			return Cell{}, ErrCorrupt
		}
		v := Version{Sequence: binary.LittleEndian.Uint64(buf[off:])}
		fl := buf[off+8]
		v.ValueLen = binary.LittleEndian.Uint32(buf[off+9:])
		off += 13
		switch {
		case fl&flagDeleted != 0:
			v.Deleted = true
			v.ValueLen = 0
		case fl&flagOverflow != 0:
			if off+4 > page.Size {
				return Cell{}, ErrCorrupt
			}
			v.OverflowPage = binary.LittleEndian.Uint32(buf[off:])
			off += 4
		default:
			n := int(v.ValueLen)
			if off+n > page.Size {
				return Cell{}, ErrCorrupt
			}
			v.Value = append([]byte(nil), buf[off:off+n]...)
			off += n
		}
		vers = append(vers, v)
	}
	return Cell{Key: key, Versions: vers}, nil
}

func ReadLeafCell(buf []byte, i int) (Cell, error) {
	if TypeOf(buf) != page.TypeBTreeLeaf {
		return Cell{}, ErrBadType
	}
	off, err := cellOffset(buf, i)
	if err != nil {
		return Cell{}, err
	}
	return decodeCell(buf, off)
}

// SearchLeaf returns the index of key, or where it would be inserted, and found.
func SearchLeaf(buf []byte, key []byte) (idx int, found bool, err error) {
	if TypeOf(buf) != page.TypeBTreeLeaf {
		return 0, false, ErrBadType
	}
	lo, hi := 0, NKeys(buf)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		k, err := keyAt(buf, mid)
		if err != nil {
			return 0, false, err
		}
		switch cmp := bytes.Compare(k, key); {
		case cmp < 0:
			lo = mid + 1
		case cmp > 0:
			hi = mid
		default:
			return mid, true, nil
		}
	}
	return lo, false, nil
}

func cellsFromLeaf(buf []byte) ([]Cell, error) {
	n := NKeys(buf)
	out := make([]Cell, 0, n)
	for i := 0; i < n; i++ {
		c, err := ReadLeafCell(buf, i)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

func leafFits(cells []Cell) bool {
	n := 0
	for _, c := range cells {
		n += cellFootprint(c)
	}
	return n <= pageCapacity
}

// writeLeaf replaces the page contents with cells, keeping sibling. Callers must
// check leafFits first; the page is only touched once the result is known to fit.
func writeLeaf(buf []byte, cells []Cell, sibling uint32) error {
	if !leafFits(cells) {
		return ErrPageFull
	}
	InitLeaf(buf)
	SetRightSibling(buf, sibling)
	for _, c := range cells {
		if err := appendEntry(buf, encodeCell(c)); err != nil {
			return err
		}
	}
	return nil
}

func spliceCell(cells []Cell, i int, c Cell, replace bool) []Cell {
	out := make([]Cell, 0, len(cells)+1)
	out = append(out, cells[:i]...)
	out = append(out, c)
	if replace {
		i++
	}
	return append(out, cells[i:]...)
}

// --- internal pages ---

func sepFootprint(key []byte) int { return 2 + len(key) + 4 + slotBytes }

func encodeSep(s sepEntry) []byte {
	dst := make([]byte, 0, 2+len(s.Key)+4)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(s.Key)))
	dst = append(dst, s.Key...)
	return binary.LittleEndian.AppendUint32(dst, s.Child)
}

func readSep(buf []byte, i int) (sepEntry, error) {
	k, err := keyAt(buf, i)
	if err != nil {
		return sepEntry{}, err
	}
	off, _ := cellOffset(buf, i)
	childOff := off + 2 + len(k)
	if childOff+4 > page.Size {
		return sepEntry{}, ErrCorrupt
	}
	return sepEntry{
		Key:   append([]byte(nil), k...),
		Child: binary.LittleEndian.Uint32(buf[childOff:]),
	}, nil
}

// SearchInternal returns the child covering key: leftChild if key < sep[0],
// else the child of the last separator <= key.
func SearchInternal(buf []byte, key []byte) (uint32, error) {
	if TypeOf(buf) != page.TypeBTreeInternal {
		return 0, ErrBadType
	}
	lo, hi := 0, NKeys(buf) // find first sep > key
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		k, err := keyAt(buf, mid)
		if err != nil {
			return 0, err
		}
		if bytes.Compare(k, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return LeftChild(buf), nil
	}
	s, err := readSep(buf, lo-1)
	if err != nil {
		return 0, err
	}
	return s.Child, nil
}

func sepsFromInternal(buf []byte) (left uint32, seps []sepEntry, err error) {
	if TypeOf(buf) != page.TypeBTreeInternal {
		return 0, nil, ErrBadType
	}
	n := NKeys(buf)
	seps = make([]sepEntry, 0, n)
	for i := 0; i < n; i++ {
		s, err := readSep(buf, i)
		if err != nil {
			return 0, nil, err
		}
		seps = append(seps, s)
	}
	return LeftChild(buf), seps, nil
}

func internalFits(seps []sepEntry) bool {
	n := 0
	for _, s := range seps {
		n += sepFootprint(s.Key)
	}
	return n <= pageCapacity
}

func writeInternal(buf []byte, left uint32, seps []sepEntry) error {
	if !internalFits(seps) {
		return ErrPageFull
	}
	InitInternal(buf)
	SetLeftChild(buf, left)
	for _, s := range seps {
		if err := appendEntry(buf, encodeSep(s)); err != nil {
			return err
		}
	}
	return nil
}

// splitPoint picks k so that sizes[:k] and sizes[k+skip:] both fit in a page,
// preferring the most even split. skip is 1 for internal splits (the middle
// separator moves up) and 0 for leaves. Requires every size <= maxCellFootprint.
func splitPoint(sizes []int, skip int) (int, bool) {
	total := 0
	for _, s := range sizes {
		total += s
	}
	best, bestDiff := -1, 0
	prefix := 0
	for k := 1; k < len(sizes); k++ {
		prefix += sizes[k-1]
		right := total - prefix
		if skip == 1 {
			right -= sizes[k]
		}
		if prefix > pageCapacity || right > pageCapacity {
			continue
		}
		diff := prefix - right
		if diff < 0 {
			diff = -diff
		}
		if best < 0 || diff < bestDiff {
			best, bestDiff = k, diff
		}
	}
	return best, best > 0
}

// VisibleVersion picks the newest version with Sequence <= snap; a tombstone hides the key.
func VisibleVersion(c Cell, snap uint64) (Version, bool) {
	for _, v := range c.Versions {
		if v.Sequence > snap {
			continue
		}
		if v.Deleted {
			return Version{}, false
		}
		return v, true
	}
	return Version{}, false
}
