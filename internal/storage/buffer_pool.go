// Package storage owns the main *.dorradb file via a buffer pool
// and heap access methods on top of it.
//
// BufferPool keeps a fixed slice of frames and an in-memory page directory
// (pageToFrame) mapping pageID → frame index for cached pages only. On-disk
// address is implicit: offset = pageID * 4096. There is no separate on-disk
// page catalog.
package storage

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"

	"dorradb/internal/encode"
	"dorradb/internal/storage/page"
	"dorradb/internal/vfs"
)

const (
	defaultCachePages = 256
	minCachePages     = 2
	defaultGrowPages  = 64 // 256 KiB chunks
)

var (
	ErrClosed      = errors.New("storage: closed")
	ErrNoFreeFrame = errors.New("storage: no free buffer frame (all pinned)")
	ErrBadPageID   = errors.New("storage: bad page id")
	ErrBadCache    = errors.New("storage: CachePages must be >= 2")
)

// OpenOptions configures the buffer pool and file growth.
type OpenOptions struct {
	// CachePages is the fixed frame count. 0 means default (256). Minimum 2.
	CachePages int
	// GrowPages is how many pages to extend the file by when the extent is
	// exhausted. 0 means default (64).
	GrowPages int
}

// BufferPool is the sole main-file I/O path: fixed 4 KiB pages, clock eviction.
// frames is the buffer pool; pageToFrame is the buffer directory (cached pages only).
type BufferPool struct {
	fs   vfs.FS
	path string
	f    vfs.File
	hdr  encode.Header

	frames      []frame
	pageToFrame map[uint32]int // page directory: pageID → frame index
	clockHand   int

	filePages uint32 // allocated extent (may be > PageCount)
	growPages uint32
	metaDirty bool
}

type frame struct {
	pageID uint32
	data   []byte
	pin    int
	dirty  bool
	ref    bool
	used   bool
}

// Open creates or opens the main DB as a paged file.
func Open(fs vfs.FS, dbPath string, create bool, opts OpenOptions) (*BufferPool, error) {
	cachePages := opts.CachePages
	if cachePages == 0 {
		cachePages = defaultCachePages
	}
	if cachePages < minCachePages {
		return nil, ErrBadCache
	}
	growPages := opts.GrowPages
	if growPages == 0 {
		growPages = defaultGrowPages
	}
	if growPages < 1 {
		growPages = 1
	}

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
		_ = f.Close()
		return nil, err
	}
	p := &BufferPool{
		fs:          fs,
		path:        dbPath,
		f:           f,
		frames:      make([]frame, cachePages),
		pageToFrame: make(map[uint32]int),
		growPages:   uint32(growPages),
	}
	for i := range p.frames {
		p.frames[i].data = make([]byte, page.Size)
	}

	if info.Size() == 0 {
		h := encode.Header{
			FileType:            encode.FileTypeMain,
			FormatVersion:       encode.FormatVersion,
			WalCheckpointOffset: encode.HeaderSize,
			PageSize:            page.Size,
			PageCount:           1,
		}
		if _, err := f.WriteAt(page.EncodeMetaPage(h), 0); err != nil {
			_ = f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, err
		}
		p.hdr = h
		p.filePages = 1
		return p, nil
	}

	meta := make([]byte, page.Size)
	if _, err := f.ReadAt(meta, 0); err != nil {
		_ = f.Close()
		return nil, err
	}
	h, err := page.DecodeMetaPage(meta)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if h.PageSize == 0 {
		h.PageSize = page.Size
	}
	if h.PageCount == 0 {
		h.PageCount = uint32(info.Size() / page.Size)
		if h.PageCount == 0 {
			h.PageCount = 1
		}
	}
	p.hdr = h
	p.filePages = uint32(info.Size() / page.Size)
	if p.filePages < p.hdr.PageCount {
		p.filePages = p.hdr.PageCount
	}
	return p, nil
}

func (p *BufferPool) Path() string          { return p.path }
func (p *BufferPool) Header() encode.Header { return p.hdr }
func (p *BufferPool) PageCount() uint32     { return p.hdr.PageCount }
func (p *BufferPool) FilePages() uint32     { return p.filePages }
func (p *BufferPool) CachePages() int       { return len(p.frames) }

// Size is the allocated file extent in bytes (may exceed logical PageCount).
func (p *BufferPool) Size() int64 { return int64(p.filePages) * page.Size }

// DirtyFrames counts dirty frames currently in the pool.
func (p *BufferPool) DirtyFrames() int {
	n := 0
	for i := range p.frames {
		if p.frames[i].used && p.frames[i].dirty {
			n++
		}
	}
	return n
}

// SetHeader updates in-memory meta and writes page 0 (no fsync).
func (p *BufferPool) SetHeader(h encode.Header) error {
	h.FileType = encode.FileTypeMain
	h.FormatVersion = encode.FormatVersion
	if h.PageSize == 0 {
		h.PageSize = page.Size
	}
	p.hdr = h
	p.metaDirty = true
	return p.writeMeta()
}

func (p *BufferPool) writeMeta() error {
	_, err := p.f.WriteAt(page.EncodeMetaPage(p.hdr), 0)
	if err == nil {
		p.metaDirty = false
	}
	return err
}

// Get pins pageID in the buffer pool (loads from disk if needed).
// Page 0 is meta-only — use Header / SetHeader.
func (p *BufferPool) Get(pageID uint32) ([]byte, error) {
	if p.f == nil {
		return nil, ErrClosed
	}
	if pageID == 0 || pageID >= p.hdr.PageCount {
		return nil, ErrBadPageID
	}
	if fi, ok := p.pageToFrame[pageID]; ok {
		fr := &p.frames[fi]
		fr.pin++
		fr.ref = true
		return fr.data, nil
	}
	fi, err := p.evict()
	if err != nil {
		return nil, err
	}
	fr := &p.frames[fi]
	if _, err := p.f.ReadAt(fr.data, int64(pageID)*page.Size); err != nil {
		return nil, err
	}
	fr.pageID = pageID
	fr.pin = 1
	fr.dirty = false
	fr.ref = true
	fr.used = true
	p.pageToFrame[pageID] = fi
	return fr.data, nil
}

// NewPage allocates a page (freelist or extend), runs initFn, and returns it pinned.
// Meta is marked dirty but not written until FlushAll / SetHeader (amortized growth).
func (p *BufferPool) NewPage(initFn func([]byte)) (uint32, []byte, error) {
	if p.f == nil {
		return 0, nil, ErrClosed
	}
	if p.hdr.FreelistHead != 0 {
		id := p.hdr.FreelistHead
		buf, err := p.Get(id)
		if err != nil {
			return 0, nil, err
		}
		p.hdr.FreelistHead = binary.LittleEndian.Uint32(buf[page.FreeNext:])
		clear(buf)
		initFn(buf)
		p.MarkDirty(id)
		// Persist freelist head without fsync (checkpoint/FlushAll syncs).
		if err := p.writeMeta(); err != nil {
			p.Unpin(id, false)
			return 0, nil, err
		}
		return id, buf, nil
	}

	if err := p.ensureCapacity(); err != nil {
		return 0, nil, err
	}

	id := p.hdr.PageCount
	p.hdr.PageCount++

	fi, err := p.evict()
	if err != nil {
		p.hdr.PageCount--
		return 0, nil, err
	}
	fr := &p.frames[fi]
	clear(fr.data)
	initFn(fr.data)
	fr.pageID = id
	fr.pin = 1
	fr.dirty = true
	fr.ref = true
	fr.used = true
	p.pageToFrame[id] = fi
	// Persist PageCount without fsync; file extent already reserved by Truncate.
	if err := p.writeMeta(); err != nil {
		delete(p.pageToFrame, id)
		fr.used = false
		p.hdr.PageCount--
		return 0, nil, err
	}
	return id, fr.data, nil
}

// ensureCapacity grows the file by growPages when the allocated extent is full.
func (p *BufferPool) ensureCapacity() error {
	if p.hdr.PageCount < p.filePages {
		return nil
	}
	add := p.growPages
	if add < 1 {
		add = 1
	}
	newFilePages := p.filePages + add
	if newFilePages <= p.filePages {
		newFilePages = p.filePages + 1
	}
	if err := p.f.Truncate(int64(newFilePages) * page.Size); err != nil {
		return err
	}
	p.filePages = newFilePages
	return nil
}

// MarkDirty marks a cached page dirty.
func (p *BufferPool) MarkDirty(pageID uint32) {
	if fi, ok := p.pageToFrame[pageID]; ok {
		p.frames[fi].dirty = true
		p.frames[fi].ref = true
	}
}

// Unpin releases a pin; dirty marks the page dirty before release.
func (p *BufferPool) Unpin(pageID uint32, dirty bool) {
	fi, ok := p.pageToFrame[pageID]
	if !ok {
		return
	}
	fr := &p.frames[fi]
	if dirty {
		fr.dirty = true
	}
	if fr.pin > 0 {
		fr.pin--
	}
}

func (p *BufferPool) evict() (int, error) {
	n := len(p.frames)
	for passes := 0; passes < 2*n; passes++ {
		i := p.clockHand
		p.clockHand = (p.clockHand + 1) % n
		fr := &p.frames[i]
		if !fr.used {
			return i, nil
		}
		if fr.pin > 0 {
			continue
		}
		if fr.ref {
			fr.ref = false
			continue
		}
		if fr.dirty {
			if err := p.writeFrame(fr); err != nil {
				return 0, err
			}
		}
		delete(p.pageToFrame, fr.pageID)
		fr.used = false
		fr.dirty = false
		fr.pageID = 0
		return i, nil
	}
	return 0, ErrNoFreeFrame
}

func (p *BufferPool) writeFrame(fr *frame) error {
	if _, err := p.f.WriteAt(fr.data, int64(fr.pageID)*page.Size); err != nil {
		return err
	}
	fr.dirty = false
	return nil
}

// Flush writes one dirty cached page (not required for commit durability).
func (p *BufferPool) Flush(pageID uint32) error {
	fi, ok := p.pageToFrame[pageID]
	if !ok || !p.frames[fi].dirty {
		return nil
	}
	return p.writeFrame(&p.frames[fi])
}

// FlushAll writes dirty pages, meta, and fsyncs the file.
func (p *BufferPool) FlushAll() error {
	for i := range p.frames {
		fr := &p.frames[i]
		if fr.used && fr.dirty {
			if err := p.writeFrame(fr); err != nil {
				return err
			}
		}
	}
	if err := p.writeMeta(); err != nil {
		return err
	}
	return p.f.Sync()
}

// FreePage pushes pageID onto the freelist (must not be pinned by the caller).
func (p *BufferPool) FreePage(pageID uint32) error {
	if pageID == 0 || pageID >= p.hdr.PageCount {
		return ErrBadPageID
	}
	buf, err := p.Get(pageID)
	if err != nil {
		return err
	}
	clear(buf)
	binary.LittleEndian.PutUint32(buf[page.FreeNext:], p.hdr.FreelistHead)
	p.hdr.FreelistHead = pageID
	p.Unpin(pageID, true)
	return p.writeMeta()
}

func (p *BufferPool) Close() error {
	if p.f == nil {
		return nil
	}
	_ = p.FlushAll()
	err := p.f.Close()
	p.f = nil
	return err
}
