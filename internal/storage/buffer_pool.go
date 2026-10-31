// Package storage owns the main *.dorradb file via a buffer pool.
//
// BufferPool keeps a fixed slice of frames and an in-memory page directory
// (pageToFrame) mapping pageID → frame index for cached pages only. On-disk
// address is implicit: offset = pageID * 4096. There is no separate on-disk
// page catalog.
//
// The pool is no-steal: dirty pages are never evicted. They reach the main
// file only through FlushAll, which first writes every dirty page (and the
// meta page) to path+"-journal", fsyncs it, then writes the main file. On open
// a complete journal is replayed, so the main file always holds the state of
// one whole FlushAll, never a mix.
package storage

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"dorradb/internal/encode"
	"dorradb/internal/storage/page"
	"dorradb/internal/vfs"
)

const (
	defaultCachePages = 256
	minCachePages     = 2
	defaultGrowPages  = 64 // 256 KiB chunks

	journalMagic uint32 = 0x4C4E524A // "JRNL"
)

var (
	ErrClosed      = errors.New("storage: closed")
	ErrNoFreeFrame = errors.New("storage: no free buffer frame (all pinned or dirty)")
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
// All methods are safe for concurrent use; callers serialize page contents themselves.
type BufferPool struct {
	mu   sync.Mutex
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

func JournalPath(dbPath string) string { return dbPath + "-journal" }

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
	if err := recoverJournal(fs, dbPath, f); err != nil {
		_ = f.Close()
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

// recoverJournal replays a complete journal into the main file, then removes it.
// A torn or corrupt journal is discarded: main was not touched before the
// journal was fsynced, so it still holds the previous flush.
func recoverJournal(fs vfs.FS, dbPath string, main vfs.File) error {
	jpath := JournalPath(dbPath)
	jf, err := fs.OpenFile(jpath, os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	info, err := jf.Stat()
	if err != nil {
		_ = jf.Close()
		return err
	}
	buf := make([]byte, info.Size())
	_, err = jf.ReadAt(buf, 0)
	_ = jf.Close()
	if err != nil && info.Size() > 0 {
		return err
	}
	entries, ok := decodeJournal(buf)
	if ok {
		for _, e := range entries {
			if _, err := main.WriteAt(e.data, int64(e.pageID)*page.Size); err != nil {
				return err
			}
		}
		if err := main.Sync(); err != nil {
			return err
		}
	}
	if err := fs.Remove(jpath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

type journalEntry struct {
	pageID uint32
	data   []byte
}

// Journal layout: magic u32 | count u32 | (pageID u32 | page bytes)* | crc32.
func encodeJournal(entries []journalEntry) []byte {
	buf := make([]byte, 0, 8+len(entries)*(4+page.Size)+4)
	buf = encode.AppendU32(buf, journalMagic)
	buf = encode.AppendU32(buf, uint32(len(entries)))
	for _, e := range entries {
		buf = encode.AppendU32(buf, e.pageID)
		buf = append(buf, e.data...)
	}
	return encode.AppendU32(buf, encode.CRC(buf))
}

func decodeJournal(buf []byte) ([]journalEntry, bool) {
	if len(buf) < 12 || binary.LittleEndian.Uint32(buf[0:4]) != journalMagic {
		return nil, false
	}
	n := int(binary.LittleEndian.Uint32(buf[4:8]))
	want := 8 + n*(4+page.Size) + 4
	if n < 0 || len(buf) != want {
		return nil, false
	}
	body := buf[:len(buf)-4]
	if encode.CRC(body) != binary.LittleEndian.Uint32(buf[len(buf)-4:]) {
		return nil, false
	}
	entries := make([]journalEntry, n)
	off := 8
	for i := range entries {
		entries[i].pageID = binary.LittleEndian.Uint32(buf[off:])
		off += 4
		entries[i].data = buf[off : off+page.Size]
		off += page.Size
	}
	return entries, true
}

func (p *BufferPool) Path() string { return p.path }

func (p *BufferPool) Header() encode.Header {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hdr
}

func (p *BufferPool) PageCount() uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hdr.PageCount
}

func (p *BufferPool) CachePages() int { return len(p.frames) }

// Size is the allocated file extent in bytes (may exceed logical PageCount).
func (p *BufferPool) Size() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return int64(p.filePages) * page.Size
}

// DirtyFrames counts dirty frames currently in the pool.
func (p *BufferPool) DirtyFrames() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for i := range p.frames {
		if p.frames[i].used && p.frames[i].dirty {
			n++
		}
	}
	return n
}

// FreeFrames counts frames that can be (re)used without a flush:
// empty, or holding a clean unpinned page.
func (p *BufferPool) FreeFrames() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for i := range p.frames {
		fr := &p.frames[i]
		if !fr.used || (!fr.dirty && fr.pin == 0) {
			n++
		}
	}
	return n
}

// SetHeader updates in-memory meta. It reaches disk on the next FlushAll.
func (p *BufferPool) SetHeader(h encode.Header) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.f == nil {
		return ErrClosed
	}
	h.FileType = encode.FileTypeMain
	h.FormatVersion = encode.FormatVersion
	if h.PageSize == 0 {
		h.PageSize = page.Size
	}
	p.hdr = h
	p.metaDirty = true
	return nil
}

// Get pins pageID in the buffer pool (loads from disk if needed).
// Page 0 is meta-only — use Header / SetHeader.
func (p *BufferPool) Get(pageID uint32) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.get(pageID)
}

func (p *BufferPool) get(pageID uint32) ([]byte, error) {
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

// NewPage allocates a page (freelist or extend), runs initFn, and returns it pinned and dirty.
func (p *BufferPool) NewPage(initFn func([]byte)) (uint32, []byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.f == nil {
		return 0, nil, ErrClosed
	}
	if p.hdr.FreelistHead != 0 {
		id := p.hdr.FreelistHead
		buf, err := p.get(id)
		if err != nil {
			return 0, nil, err
		}
		p.hdr.FreelistHead = binary.LittleEndian.Uint32(buf[page.FreeNext:])
		p.metaDirty = true
		clear(buf)
		initFn(buf)
		p.frames[p.pageToFrame[id]].dirty = true
		return id, buf, nil
	}

	if err := p.ensureCapacity(); err != nil {
		return 0, nil, err
	}
	fi, err := p.evict()
	if err != nil {
		return 0, nil, err
	}
	id := p.hdr.PageCount
	p.hdr.PageCount++
	p.metaDirty = true
	fr := &p.frames[fi]
	clear(fr.data)
	initFn(fr.data)
	fr.pageID = id
	fr.pin = 1
	fr.dirty = true
	fr.ref = true
	fr.used = true
	p.pageToFrame[id] = fi
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

// Unpin releases a pin; dirty marks the page dirty before release.
func (p *BufferPool) Unpin(pageID uint32, dirty bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
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

// evict returns a free frame. Dirty frames are never evicted (no-steal).
func (p *BufferPool) evict() (int, error) {
	n := len(p.frames)
	for passes := 0; passes < 2*n; passes++ {
		i := p.clockHand
		p.clockHand = (p.clockHand + 1) % n
		fr := &p.frames[i]
		if !fr.used {
			return i, nil
		}
		if fr.pin > 0 || fr.dirty {
			continue
		}
		if fr.ref {
			fr.ref = false
			continue
		}
		delete(p.pageToFrame, fr.pageID)
		fr.used = false
		fr.pageID = 0
		return i, nil
	}
	return 0, ErrNoFreeFrame
}

// FlushAll atomically writes all dirty pages and meta (via the journal) and fsyncs.
func (p *BufferPool) FlushAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.flushAll()
}

func (p *BufferPool) flushAll() error {
	if p.f == nil {
		return ErrClosed
	}
	var entries []journalEntry
	var dirty []*frame
	for i := range p.frames {
		fr := &p.frames[i]
		if fr.used && fr.dirty {
			entries = append(entries, journalEntry{pageID: fr.pageID, data: fr.data})
			dirty = append(dirty, fr)
		}
	}
	if p.metaDirty {
		entries = append(entries, journalEntry{pageID: 0, data: page.EncodeMetaPage(p.hdr)})
	}
	if len(entries) == 0 {
		return p.f.Sync()
	}

	jpath := JournalPath(p.path)
	jf, err := p.fs.OpenFile(jpath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := jf.WriteAt(encodeJournal(entries), 0); err != nil {
		_ = jf.Close()
		return err
	}
	if err := jf.Sync(); err != nil {
		_ = jf.Close()
		return err
	}
	if err := jf.Close(); err != nil {
		return err
	}
	_ = p.fs.SyncDir(jpath)

	for _, e := range entries {
		if _, err := p.f.WriteAt(e.data, int64(e.pageID)*page.Size); err != nil {
			return err
		}
	}
	if err := p.f.Sync(); err != nil {
		return err
	}
	for _, fr := range dirty {
		fr.dirty = false
	}
	p.metaDirty = false
	if err := p.fs.Remove(jpath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// FreePage pushes pageID onto the freelist (must not be pinned by the caller).
func (p *BufferPool) FreePage(pageID uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pageID == 0 || pageID >= p.hdr.PageCount {
		return ErrBadPageID
	}
	buf, err := p.get(pageID)
	if err != nil {
		return err
	}
	clear(buf)
	binary.LittleEndian.PutUint32(buf[page.FreeNext:], p.hdr.FreelistHead)
	p.hdr.FreelistHead = pageID
	p.metaDirty = true
	fr := &p.frames[p.pageToFrame[pageID]]
	fr.dirty = true
	fr.pin--
	return nil
}

func (p *BufferPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.f == nil {
		return nil
	}
	ferr := p.flushAll()
	err := p.f.Close()
	p.f = nil
	if ferr != nil {
		return ferr
	}
	return err
}
