// Package storage owns the main *.dorradb file: append records, ReadAt values,
// scan into the RAM index, and replace the file during checkpoint compact.
package storage

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"

	"dorradb/internal/encode"
	"dorradb/internal/index"
	"dorradb/internal/vfs"
)

// Main is the open main database file (header + append-only framed records).
type Main struct {
	fs   vfs.FS
	path string
	f    vfs.File
	size int64
	hdr  encode.Header
}

// Open creates or opens the main DB file through the VFS.
func Open(fs vfs.FS, dbPath string, create bool) (*Main, error) {
	if fs == nil {
		fs = vfs.OS()
	}
	flag := os.O_RDWR
	if create {
		flag |= os.O_CREATE
	}
	dir := filepath.Dir(dbPath)
	if dir != "." && dir != "" {
		if err := fs.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	f, err := fs.OpenFile(dbPath, flag, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	m := &Main{fs: fs, path: dbPath, f: f, size: info.Size()}
	if m.size == 0 {
		h := encode.Header{
			FileType:            encode.FileTypeMain,
			FormatVersion:       encode.FormatVersion,
			Sequence:            0,
			WalCheckpointOffset: encode.HeaderSize,
		}
		if _, err := f.Write(encode.EncodeHeader(h)); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
		m.size = encode.HeaderSize
		m.hdr = h
		return m, nil
	}
	hdr := make([]byte, encode.HeaderSize)
	if _, err := f.ReadAt(hdr, 0); err != nil {
		f.Close()
		return nil, err
	}
	h, err := encode.DecodeHeader(hdr)
	if err != nil {
		f.Close()
		return nil, err
	}
	if h.FileType != encode.FileTypeMain {
		f.Close()
		return nil, encode.ErrFileType
	}
	m.hdr = h
	return m, nil
}

func (m *Main) Path() string          { return m.path }
func (m *Main) Size() int64           { return m.size }
func (m *Main) Header() encode.Header { return m.hdr }

func (m *Main) SetHeader(h encode.Header) error {
	h.FileType = encode.FileTypeMain
	h.FormatVersion = encode.FormatVersion
	buf := encode.EncodeHeader(h)
	if _, err := m.f.WriteAt(buf, 0); err != nil {
		return err
	}
	if err := m.f.Sync(); err != nil {
		return err
	}
	m.hdr = h
	return nil
}

// Append writes one framed record at the end of the file and returns its Loc for the index.
func (m *Main) Append(rec encode.Record) (index.Loc, error) {
	frame := encode.EncodeFrame(rec)
	off := m.size
	if _, err := m.f.WriteAt(frame, off); err != nil {
		return index.Loc{}, err
	}
	m.size += int64(len(frame))
	fv, _, err := encode.DecodeFrameAt(frame, off)
	if err != nil {
		return index.Loc{}, err
	}
	loc := index.Loc{
		Offset:   fv.Offset,
		Length:   fv.Length,
		ValueOff: fv.ValueOff,
		ValueLen: fv.ValueLen,
		Sequence: rec.Sequence,
		Deleted:  rec.Op == encode.OpDelete,
	}
	return loc, nil
}

// ReadValue loads value bytes from disk using the index Loc (not from RAM).
func (m *Main) ReadValue(loc index.Loc) ([]byte, error) {
	if loc.Deleted || loc.ValueLen == 0 {
		return nil, nil
	}
	buf := make([]byte, loc.ValueLen)
	if _, err := m.f.ReadAt(buf, loc.ValueOff); err != nil {
		return nil, err
	}
	return buf, nil
}

// ScanIndex rebuilds the RAM index by reading every good frame.
// Stops at a torn/corrupt tail (crash mid-write); mid-file CRC → ErrMidCRC.
func (m *Main) ScanIndex(idx *index.Index) error {
	idx.Reset()
	if m.size <= encode.HeaderSize {
		return nil
	}
	buf := make([]byte, m.size-encode.HeaderSize)
	if _, err := m.f.ReadAt(buf, encode.HeaderSize); err != nil && err != io.EOF {
		return err
	}
	off := int64(encode.HeaderSize)
	pos := 0
	for pos < len(buf) {
		fv, n, err := encode.DecodeFrameAt(buf[pos:], off)
		if err != nil {
			if err == encode.ErrTorn || err == encode.ErrShort {
				return nil
			}
			if err == encode.ErrCRC {
				if pos+4 <= len(buf) {
					plen := int(binary.LittleEndian.Uint32(buf[pos : pos+4]))
					total := encode.FrameSize(plen)
					if plen >= 0 && pos+total >= len(buf) {
						return nil
					}
				}
				return encode.ErrMidCRC
			}
			return err
		}
		loc := index.Loc{
			Offset:   fv.Offset,
			Length:   fv.Length,
			ValueOff: fv.ValueOff,
			ValueLen: fv.ValueLen,
			Sequence: fv.Record.Sequence,
			Deleted:  fv.Record.Op == encode.OpDelete,
		}
		idx.Apply(fv.Record.Key, loc)
		pos += n
		off += int64(n)
	}
	return nil
}

// ReplaceWithCompact atomically swaps in a compacted file:
// close old → rename tmp over path → SyncDir → reopen.
func (m *Main) ReplaceWithCompact(tmpPath string, h encode.Header, newSize int64) error {
	if err := m.f.Close(); err != nil {
		return err
	}
	m.f = nil
	if err := m.fs.Rename(tmpPath, m.path); err != nil {
		return err
	}
	if err := m.fs.SyncDir(m.path); err != nil {
		return err
	}
	f, err := m.fs.OpenFile(m.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	m.f = f
	m.size = newSize
	m.hdr = h
	return nil
}

func (m *Main) Close() error {
	if m.f == nil {
		return nil
	}
	err := m.f.Close()
	m.f = nil
	return err
}

// WriteCompactFile writes header + only live Set records to a temp file and fsyncs it.
func WriteCompactFile(fs vfs.FS, tmpPath string, h encode.Header, records []encode.Record) (int64, error) {
	if fs == nil {
		fs = vfs.OS()
	}
	f, err := fs.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	h.FileType = encode.FileTypeMain
	h.FormatVersion = encode.FormatVersion
	if _, err := f.Write(encode.EncodeHeader(h)); err != nil {
		return 0, err
	}
	var size int64 = encode.HeaderSize
	for _, rec := range records {
		frame := encode.EncodeFrame(rec)
		if _, err := f.Write(frame); err != nil {
			return 0, err
		}
		size += int64(len(frame))
	}
	if err := f.Sync(); err != nil {
		return 0, err
	}
	return size, nil
}
