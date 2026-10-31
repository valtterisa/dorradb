// Package encode is the on-disk binary format: file headers and framed records.
// Everything is little-endian. Each record is: len | payload | CRC32.
package encode

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	// Magic is ASCII "DORA" stored as a little-endian uint32.
	Magic         uint32 = 0x41524F44
	FileTypeMain  uint8  = 1
	FileTypeWAL   uint8  = 2
	FormatVersion uint8  = 3
	HeaderSize           = 64 // fixed header at the start of main + WAL files
	DefaultPageSize      = 4096

	OpSet    uint8 = 1
	OpDelete uint8 = 2
)

var (
	ErrShort    = errors.New("encode: short buffer")
	ErrCRC      = errors.New("encode: crc mismatch")
	ErrMagic    = errors.New("encode: bad magic")
	ErrVersion  = errors.New("encode: unsupported version")
	ErrFileType = errors.New("encode: bad file type")
	ErrTorn     = errors.New("encode: torn record")          // incomplete last record (crash)
	ErrMidCRC   = errors.New("encode: mid-file crc failure") // corrupt, not just a torn tail
)

var crcTable = crc32.MakeTable(crc32.IEEE)

// Header sits at offset 0 of main and WAL files (and page 0 of the main DB).
type Header struct {
	FileType            uint8
	FormatVersion       uint8
	Sequence            uint64 // highest sequence flushed to main pages
	WalCheckpointOffset uint64 // WAL byte offset where replay should start
	PageSize            uint32 // main file only; 0 on WAL
	PageCount           uint32 // main file only; includes page 0
	FreelistHead        uint32 // main file only; 0 = empty
	TableRoot           uint32 // main file only; clustered B+ tree root page id (0 = none)
}

func AppendU32(b []byte, v uint32) []byte {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], v)
	return append(b, tmp[:]...)
}

func AppendU64(b []byte, v uint64) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	return append(b, tmp[:]...)
}

func CRC(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}

// EncodeHeader writes a 64-byte header with a CRC over the first 60 bytes.
func EncodeHeader(h Header) []byte {
	buf := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], Magic)
	buf[4] = h.FileType
	buf[5] = h.FormatVersion
	binary.LittleEndian.PutUint64(buf[8:16], h.Sequence)
	binary.LittleEndian.PutUint64(buf[16:24], h.WalCheckpointOffset)
	binary.LittleEndian.PutUint32(buf[24:28], h.PageSize)
	binary.LittleEndian.PutUint32(buf[28:32], h.PageCount)
	binary.LittleEndian.PutUint32(buf[32:36], h.FreelistHead)
	binary.LittleEndian.PutUint32(buf[36:40], h.TableRoot)
	sum := CRC(buf[:HeaderSize-4])
	binary.LittleEndian.PutUint32(buf[HeaderSize-4:], sum)
	return buf
}

func DecodeHeader(buf []byte) (Header, error) {
	if len(buf) < HeaderSize {
		return Header{}, ErrShort
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != Magic {
		return Header{}, ErrMagic
	}
	want := binary.LittleEndian.Uint32(buf[HeaderSize-4:])
	if CRC(buf[:HeaderSize-4]) != want {
		return Header{}, ErrCRC
	}
	h := Header{
		FileType:            buf[4],
		FormatVersion:       buf[5],
		Sequence:            binary.LittleEndian.Uint64(buf[8:16]),
		WalCheckpointOffset: binary.LittleEndian.Uint64(buf[16:24]),
		PageSize:            binary.LittleEndian.Uint32(buf[24:28]),
		PageCount:           binary.LittleEndian.Uint32(buf[28:32]),
		FreelistHead:        binary.LittleEndian.Uint32(buf[32:36]),
		TableRoot:           binary.LittleEndian.Uint32(buf[36:40]),
	}
	// Version gate: refuse anything we don't understand.
	if h.FormatVersion != FormatVersion {
		return Header{}, ErrVersion
	}
	return h, nil
}

// Record is one logical mutation (also what we store in the main data file).
type Record struct {
	Sequence uint64 // monotonic logical commit/version number (MVCC write stamp)
	Op       uint8
	Key      []byte
	Value    []byte // empty for Delete
}

// Payload layout: sequence u64 | op u8 | keyLen u32 | key | valLen u32 | value
func EncodePayload(r Record) []byte {
	keyLen := uint32(len(r.Key))
	valLen := uint32(len(r.Value))
	n := 8 + 1 + 4 + len(r.Key) + 4 + len(r.Value)
	buf := make([]byte, 0, n)
	buf = AppendU64(buf, r.Sequence)
	buf = append(buf, r.Op)
	buf = AppendU32(buf, keyLen)
	buf = append(buf, r.Key...)
	buf = AppendU32(buf, valLen)
	buf = append(buf, r.Value...)
	return buf
}

func DecodePayload(p []byte) (Record, error) {
	if len(p) < 8+1+4+4 {
		return Record{}, ErrShort
	}
	r := Record{Sequence: binary.LittleEndian.Uint64(p[0:8]), Op: p[8]}
	off := 9
	if off+4 > len(p) {
		return Record{}, ErrShort
	}
	keyLen := int(binary.LittleEndian.Uint32(p[off : off+4]))
	off += 4
	if keyLen < 0 || off+keyLen+4 > len(p) {
		return Record{}, ErrShort
	}
	r.Key = append([]byte(nil), p[off:off+keyLen]...)
	off += keyLen
	valLen := int(binary.LittleEndian.Uint32(p[off : off+4]))
	off += 4
	if valLen < 0 || off+valLen > len(p) {
		return Record{}, ErrShort
	}
	if valLen > 0 {
		r.Value = append([]byte(nil), p[off:off+valLen]...)
	}
	return r, nil
}

// EncodeFrame wraps payload as: len u32 | payload | crc32.
func EncodeFrame(r Record) []byte {
	payload := EncodePayload(r)
	frame := make([]byte, 0, 4+len(payload)+4)
	frame = AppendU32(frame, uint32(len(payload)))
	frame = append(frame, payload...)
	frame = AppendU32(frame, CRC(payload))
	return frame
}

const FrameOverhead = 8 // len + crc

func FrameSize(payloadLen int) int {
	return 4 + payloadLen + 4
}

// FrameView is a decoded frame plus where it lives in the file (for ReadAt of values).
type FrameView struct {
	Record     Record
	Offset     int64 // start of frame in file
	Length     int64 // full frame size
	ValueOff   int64 // absolute offset of value bytes
	ValueLen   int
	PayloadOff int64
}

// DecodeFrameAt parses one frame from data (usually a slice of the file).
// fileOff is the absolute file offset of data[0] so ValueOff is absolute too.
func DecodeFrameAt(data []byte, fileOff int64) (FrameView, int, error) {
	if len(data) < 4 {
		return FrameView{}, 0, ErrTorn
	}
	plen := int(binary.LittleEndian.Uint32(data[0:4]))
	total := FrameSize(plen)
	if plen < 0 || len(data) < total {
		return FrameView{}, 0, ErrTorn
	}
	payload := data[4 : 4+plen]
	want := binary.LittleEndian.Uint32(data[4+plen : 4+plen+4])
	if CRC(payload) != want {
		return FrameView{}, 0, ErrCRC
	}
	rec, err := DecodePayload(payload)
	if err != nil {
		return FrameView{}, 0, err
	}
	// value starts after sequence+op+keyLen+key+valLen inside the payload.
	valueOffInPayload := 8 + 1 + 4 + len(rec.Key) + 4
	fv := FrameView{
		Record:     rec,
		Offset:     fileOff,
		Length:     int64(total),
		ValueOff:   fileOff + 4 + int64(valueOffInPayload),
		ValueLen:   len(rec.Value),
		PayloadOff: fileOff + 4,
	}
	return fv, total, nil
}
