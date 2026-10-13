// Package page defines fixed-size page layouts for the main DB file.
// Page types are extensible so B-tree nodes can share the pager later.
package page

import (
	"encoding/binary"
	"errors"

	"dorradb/internal/encode"
)

const (
	Size = encode.DefaultPageSize // 4096

	TypeMeta          uint8 = 1
	TypeHeap          uint8 = 2
	TypeOverflow      uint8 = 3
	TypeBTreeLeaf     uint8 = 4 // reserved
	TypeBTreeInternal uint8 = 5 // reserved

	// Heap page header (bytes 0..HeapHdrSize-1).
	hdrType     = 0
	hdrNSlots   = 2 // u16
	hdrLower    = 4 // u16 — end of record payload
	hdrUpper    = 6 // u16 — start of slot directory
	hdrSequence = 8 // u64
	HeapHdrSize = 16

	flagOverflow uint8 = 1
	slotBytes          = 2

	// Freelist / overflow "next" page id (u32 at byte 2).
	FreeNext = 2
	ovNext   = 2
	ovLen    = 6
	ovData   = 8
)

// OverflowCapacity is max payload bytes on an overflow page.
const OverflowCapacity = Size - ovData

var (
	ErrPageFull = errors.New("page: no space")
	ErrBadSlot  = errors.New("page: bad slot")
	ErrBadType  = errors.New("page: bad page type")
	ErrCorrupt  = errors.New("page: corrupt")
	ErrShort    = errors.New("page: short buffer")
)

// InitHeap zeroes a page as an empty slotted heap page.
func InitHeap(buf []byte) {
	mustPage(buf)
	clear(buf)
	buf[hdrType] = TypeHeap
	binary.LittleEndian.PutUint16(buf[hdrLower:], HeapHdrSize)
	binary.LittleEndian.PutUint16(buf[hdrUpper:], Size)
}

// InitOverflow zeroes a page as an overflow page.
func InitOverflow(buf []byte) {
	mustPage(buf)
	clear(buf)
	buf[hdrType] = TypeOverflow
}

func mustPage(buf []byte) {
	if len(buf) < Size {
		panic("page: buffer smaller than page size")
	}
}

func TypeOf(buf []byte) uint8 {
	if len(buf) < 1 {
		return 0
	}
	return buf[hdrType]
}

func FreeSpace(buf []byte) int {
	lower := int(binary.LittleEndian.Uint16(buf[hdrLower:]))
	upper := int(binary.LittleEndian.Uint16(buf[hdrUpper:]))
	return upper - lower - slotBytes
}

// Record is one heap-page payload (logical set/delete).
type Record struct {
	Sequence     uint64
	Op           uint8
	Key          []byte
	Value        []byte // inline only; empty when OverflowPage != 0
	OverflowPage uint32 // 0 = inline value
	ValueLen     uint32 // total value length (inline or overflow)
}

// RecordSize is encoded size on a heap page (excluding the slot directory entry).
func RecordSize(r Record) int {
	n := 8 + 1 + 1 + 2 + len(r.Key)
	if r.OverflowPage != 0 {
		return n + 4 + 4
	}
	return n + 4 + int(r.ValueLen)
}

// Insert appends a record into a heap page. Returns slot index.
func Insert(buf []byte, r Record) (uint16, error) {
	if TypeOf(buf) != TypeHeap {
		return 0, ErrBadType
	}
	if r.OverflowPage == 0 && uint32(len(r.Value)) != r.ValueLen {
		r.ValueLen = uint32(len(r.Value))
	}
	need := RecordSize(r)
	if FreeSpace(buf) < need {
		return 0, ErrPageFull
	}
	lower := int(binary.LittleEndian.Uint16(buf[hdrLower:]))
	upper := int(binary.LittleEndian.Uint16(buf[hdrUpper:]))
	nslots := binary.LittleEndian.Uint16(buf[hdrNSlots:])

	off := lower
	flags := uint8(0)
	if r.OverflowPage != 0 {
		flags = flagOverflow
	}
	binary.LittleEndian.PutUint64(buf[off:], r.Sequence)
	off += 8
	buf[off] = r.Op
	off++
	buf[off] = flags
	off++
	binary.LittleEndian.PutUint16(buf[off:], uint16(len(r.Key)))
	off += 2
	copy(buf[off:], r.Key)
	off += len(r.Key)
	if r.OverflowPage != 0 {
		binary.LittleEndian.PutUint32(buf[off:], r.OverflowPage)
		off += 4
		binary.LittleEndian.PutUint32(buf[off:], r.ValueLen)
		off += 4
	} else {
		binary.LittleEndian.PutUint32(buf[off:], r.ValueLen)
		off += 4
		copy(buf[off:], r.Value)
		off += int(r.ValueLen)
	}

	newUpper := upper - slotBytes
	binary.LittleEndian.PutUint16(buf[newUpper:], uint16(lower))
	binary.LittleEndian.PutUint16(buf[hdrLower:], uint16(off))
	binary.LittleEndian.PutUint16(buf[hdrUpper:], uint16(newUpper))
	binary.LittleEndian.PutUint16(buf[hdrNSlots:], nslots+1)
	if r.Sequence > binary.LittleEndian.Uint64(buf[hdrSequence:]) {
		binary.LittleEndian.PutUint64(buf[hdrSequence:], r.Sequence)
	}
	return nslots, nil
}

// ReadSlot decodes the record at slot (overflow body not followed).
func ReadSlot(buf []byte, slot uint16) (Record, int, error) {
	if TypeOf(buf) != TypeHeap {
		return Record{}, 0, ErrBadType
	}
	nslots := binary.LittleEndian.Uint16(buf[hdrNSlots:])
	if slot >= nslots {
		return Record{}, 0, ErrBadSlot
	}
	upper := int(binary.LittleEndian.Uint16(buf[hdrUpper:]))
	slotOff := upper + int(slot)*slotBytes
	if slotOff+slotBytes > Size {
		return Record{}, 0, ErrCorrupt
	}
	recOff := int(binary.LittleEndian.Uint16(buf[slotOff:]))
	return decodeRecordAt(buf, recOff)
}

func decodeRecordAt(buf []byte, off int) (Record, int, error) {
	start := off
	if off+12 > Size {
		return Record{}, 0, ErrCorrupt
	}
	var r Record
	r.Sequence = binary.LittleEndian.Uint64(buf[off:])
	off += 8
	r.Op = buf[off]
	off++
	flags := buf[off]
	off++
	keyLen := int(binary.LittleEndian.Uint16(buf[off:]))
	off += 2
	if keyLen < 0 || off+keyLen+4 > Size {
		return Record{}, 0, ErrCorrupt
	}
	r.Key = append([]byte(nil), buf[off:off+keyLen]...)
	off += keyLen
	if flags&flagOverflow != 0 {
		if off+8 > Size {
			return Record{}, 0, ErrCorrupt
		}
		r.OverflowPage = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		r.ValueLen = binary.LittleEndian.Uint32(buf[off:])
		off += 4
	} else {
		if off+4 > Size {
			return Record{}, 0, ErrCorrupt
		}
		r.ValueLen = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		if off+int(r.ValueLen) > Size {
			return Record{}, 0, ErrCorrupt
		}
		if r.ValueLen > 0 {
			r.Value = append([]byte(nil), buf[off:off+int(r.ValueLen)]...)
		}
		off += int(r.ValueLen)
	}
	return r, off - start, nil
}

// NSlots returns the number of slots on a heap page.
func NSlots(buf []byte) uint16 {
	return binary.LittleEndian.Uint16(buf[hdrNSlots:])
}

// WriteOverflowChunk fills an overflow page; next=0 means last chunk.
func WriteOverflowChunk(buf []byte, next uint32, data []byte) error {
	mustPage(buf)
	if len(data) > OverflowCapacity {
		return ErrPageFull
	}
	clear(buf)
	buf[hdrType] = TypeOverflow
	binary.LittleEndian.PutUint32(buf[ovNext:], next)
	binary.LittleEndian.PutUint16(buf[ovLen:], uint16(len(data)))
	copy(buf[ovData:], data)
	return nil
}

// ReadOverflowChunk returns the next page id and this chunk's bytes.
func ReadOverflowChunk(buf []byte) (next uint32, data []byte, err error) {
	if TypeOf(buf) != TypeOverflow {
		return 0, nil, ErrBadType
	}
	next = binary.LittleEndian.Uint32(buf[ovNext:])
	n := int(binary.LittleEndian.Uint16(buf[ovLen:]))
	if n < 0 || ovData+n > Size {
		return 0, nil, ErrCorrupt
	}
	data = append([]byte(nil), buf[ovData:ovData+n]...)
	return next, data, nil
}

// EncodeMetaPage writes page 0: 64-byte file header, TypeMeta marker, rest zero.
func EncodeMetaPage(h encode.Header) []byte {
	buf := make([]byte, Size)
	h.FileType = encode.FileTypeMain
	h.FormatVersion = encode.FormatVersion
	if h.PageSize == 0 {
		h.PageSize = Size
	}
	if h.PageCount == 0 {
		h.PageCount = 1
	}
	copy(buf, encode.EncodeHeader(h))
	buf[encode.HeaderSize] = TypeMeta
	return buf
}

// DecodeMetaPage reads the file header from page 0.
func DecodeMetaPage(buf []byte) (encode.Header, error) {
	if len(buf) < encode.HeaderSize {
		return encode.Header{}, ErrShort
	}
	h, err := encode.DecodeHeader(buf[:encode.HeaderSize])
	if err != nil {
		return encode.Header{}, err
	}
	if h.FileType != encode.FileTypeMain {
		return encode.Header{}, encode.ErrFileType
	}
	if h.PageSize != 0 && h.PageSize != Size {
		return encode.Header{}, ErrCorrupt
	}
	return h, nil
}
