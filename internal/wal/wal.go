// Package wal is the write-ahead log (path+"-wal").
// Durability rule: append mutation frames (+ OpCommit), Sync, then the engine applies to main.
package wal

import (
	"io"
	"os"

	"dorradb/internal/encode"
	"dorradb/internal/vfs"
)

// Log is the open WAL file.
type Log struct {
	fs      vfs.FS
	path    string
	f       vfs.File
	size    int64
	version uint8
}

func PathFor(dbPath string) string {
	return dbPath + "-wal"
}

// Open creates or opens the WAL sidecar next to the main DB file.
func Open(fs vfs.FS, dbPath string, create bool) (*Log, error) {
	if fs == nil {
		fs = vfs.OS()
	}
	path := PathFor(dbPath)
	flag := os.O_RDWR
	if create {
		flag |= os.O_CREATE
	}
	f, err := fs.OpenFile(path, flag, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	w := &Log{fs: fs, path: path, f: f, size: info.Size()}
	if w.size == 0 {
		h := encode.Header{FileType: encode.FileTypeWAL, FormatVersion: encode.FormatVersion}
		if _, err := f.Write(encode.EncodeHeader(h)); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
		w.size = encode.HeaderSize
		w.version = encode.FormatVersion
	} else {
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
		if h.FileType != encode.FileTypeWAL {
			f.Close()
			return nil, encode.ErrFileType
		}
		w.version = h.FormatVersion
	}
	return w, nil
}

func (w *Log) Path() string { return w.path }

func (w *Log) Size() int64 { return w.size }

// Version is the on-disk FormatVersion from the WAL header.
func (w *Log) Version() uint8 { return w.version }

// Append writes one frame (no fsync — caller Syncs once per commit).
func (w *Log) Append(rec encode.Record) (int64, int, error) {
	frame := encode.EncodeFrame(rec)
	off := w.size
	if _, err := w.f.WriteAt(frame, off); err != nil {
		return 0, 0, err
	}
	w.size += int64(len(frame))
	return off, len(frame), nil
}

func (w *Log) Sync() error {
	return w.f.Sync()
}

// Replay walks frames from fromOffset to EOF.
// Stops cleanly at the first frame that fails to decode (torn tail, CRC, etc.).
// Anything after the first invalid frame is discarded on the next TruncateReset.
func (w *Log) Replay(fromOffset int64, fn func(encode.FrameView) error) error {
	if fromOffset < encode.HeaderSize {
		fromOffset = encode.HeaderSize
	}
	info, err := w.f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	if fromOffset >= size {
		return nil
	}
	buf := make([]byte, size-fromOffset)
	if _, err := w.f.ReadAt(buf, fromOffset); err != nil && err != io.EOF {
		return err
	}
	off := fromOffset
	pos := 0
	for pos < len(buf) {
		fv, n, err := encode.DecodeFrameAt(buf[pos:], off)
		if err != nil {
			// Stop at first invalid frame (SQLite-style). Tail is unsynced/unacked.
			return nil
		}
		if err := fn(fv); err != nil {
			return err
		}
		pos += n
		off += int64(n)
	}
	return nil
}

// TruncateReset clears the WAL after a successful checkpoint (header only, current format).
func (w *Log) TruncateReset() error {
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	h := encode.Header{FileType: encode.FileTypeWAL, FormatVersion: encode.FormatVersion}
	if _, err := w.f.Write(encode.EncodeHeader(h)); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.size = encode.HeaderSize
	w.version = encode.FormatVersion
	return nil
}

func (w *Log) Close() error {
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}
