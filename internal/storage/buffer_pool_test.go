package storage

import (
	"os"
	"path/filepath"
	"testing"

	"dorradb/internal/encode"
	"dorradb/internal/storage/page"
	"dorradb/internal/vfs"
)

func TestBufferPoolGetDirtyFlush(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dorradb")
	s, err := OpenStore(vfs.OS(), path, true, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id, buf, err := s.pool.NewPage(page.InitHeap)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := page.Insert(buf, page.Record{
		Sequence: 1,
		Op:       encode.OpSet,
		Key:      []byte("k"),
		Value:    []byte("v"),
		ValueLen: 1,
	}); err != nil {
		t.Fatal(err)
	}
	s.pool.Unpin(id, true)
	if err := s.pool.FlushAll(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenStore(vfs.OS(), path, false, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	buf2, err := s2.pool.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	rec, _, err := page.ReadSlot(buf2, 0)
	s2.pool.Unpin(id, false)
	if err != nil {
		t.Fatal(err)
	}
	if string(rec.Key) != "k" || string(rec.Value) != "v" {
		t.Fatalf("got %+v", rec)
	}
}

func TestBufferPoolDirtyEvictionWriteback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dorradb")
	p, err := Open(vfs.OS(), path, true, OpenOptions{CachePages: 2, GrowPages: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	id1, buf1, err := p.NewPage(page.InitHeap)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := page.Insert(buf1, page.Record{
		Sequence: 1, Op: encode.OpSet, Key: []byte("a"), Value: []byte("1"), ValueLen: 1,
	}); err != nil {
		t.Fatal(err)
	}
	p.Unpin(id1, true)

	id2, _, err := p.NewPage(page.InitHeap)
	if err != nil {
		t.Fatal(err)
	}
	p.Unpin(id2, true)
	// No-steal: both frames are dirty, so nothing can be evicted until a flush.
	if _, _, err := p.NewPage(page.InitHeap); err != ErrNoFreeFrame {
		t.Fatalf("got %v want ErrNoFreeFrame", err)
	}
	if err := p.FlushAll(); err != nil {
		t.Fatal(err)
	}
	id3, _, err := p.NewPage(page.InitHeap)
	if err != nil {
		t.Fatal(err)
	}
	p.Unpin(id3, true)

	buf, err := p.Get(id1)
	if err != nil {
		t.Fatal(err)
	}
	rec, _, err := page.ReadSlot(buf, 0)
	p.Unpin(id1, false)
	if err != nil {
		t.Fatal(err)
	}
	if string(rec.Key) != "a" || string(rec.Value) != "1" {
		t.Fatalf("got %+v", rec)
	}
}

func TestBufferPoolErrNoFreeFrame(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dorradb")
	p, err := Open(vfs.OS(), path, true, OpenOptions{CachePages: 2, GrowPages: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	id1, _, err := p.NewPage(page.InitHeap)
	if err != nil {
		t.Fatal(err)
	}
	id2, _, err := p.NewPage(page.InitHeap)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = p.NewPage(page.InitHeap)
	if err != ErrNoFreeFrame {
		t.Fatalf("got %v want ErrNoFreeFrame", err)
	}
	p.Unpin(id1, false)
	p.Unpin(id2, false)
}

func TestBufferPoolRejectSmallCache(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dorradb")
	_, err := Open(vfs.OS(), path, true, OpenOptions{CachePages: 1})
	if err != ErrBadCache {
		t.Fatalf("got %v", err)
	}
}

func TestUnflushedPagesNeverReachDisk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dorradb")
	p, err := Open(vfs.OS(), path, true, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	id, buf, err := p.NewPage(page.InitHeap)
	if err != nil {
		t.Fatal(err)
	}
	buf[100] = 0xAB
	p.Unpin(id, true)
	h := p.Header()
	h.TableRoot = id
	_ = p.SetHeader(h)

	// Without a flush the main file must still be the empty meta page.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	onDisk, err := page.DecodeMetaPage(raw)
	if err != nil {
		t.Fatal(err)
	}
	if onDisk.PageCount != 1 || onDisk.TableRoot != 0 {
		t.Fatalf("meta leaked to disk before flush: %+v", onDisk)
	}
	_ = p.Close()
}

func TestTornJournalIsIgnored(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dorradb")
	p, err := Open(vfs.OS(), path, true, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = p.Close()
	before, _ := os.ReadFile(path)

	torn := encodeJournal([]journalEntry{{pageID: 0, data: make([]byte, page.Size)}})
	if err := os.WriteFile(JournalPath(path), torn[:len(torn)-10], 0o644); err != nil {
		t.Fatal(err)
	}
	p2, err := Open(vfs.OS(), path, false, OpenOptions{})
	if err != nil {
		t.Fatalf("open with torn journal: %v", err)
	}
	_ = p2.Close()
	after, _ := os.ReadFile(path)
	if string(before[:page.Size]) != string(after[:page.Size]) {
		t.Fatal("torn journal was applied")
	}
	if _, err := os.Stat(JournalPath(path)); !os.IsNotExist(err) {
		t.Fatal("journal not removed")
	}
}

func TestCompleteJournalIsReplayed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dorradb")
	p, err := Open(vfs.OS(), path, true, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	h := p.Header()
	_ = p.Close()

	// Simulate a crash after the journal fsync but before main was written.
	h.Sequence = 42
	if err := os.WriteFile(JournalPath(path), encodeJournal([]journalEntry{{pageID: 0, data: page.EncodeMetaPage(h)}}), 0o644); err != nil {
		t.Fatal(err)
	}
	p2, err := Open(vfs.OS(), path, false, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if p2.Header().Sequence != 42 {
		t.Fatalf("journal not replayed: seq=%d", p2.Header().Sequence)
	}
}

func TestHeaderTableRoot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dorradb")
	s, err := OpenStore(vfs.OS(), path, true, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	h := s.Header()
	h.TableRoot = 7
	if err := s.SetHeader(h); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	s2, err := OpenStore(vfs.OS(), path, false, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if s2.Header().TableRoot != 7 {
		t.Fatalf("TableRoot=%d", s2.Header().TableRoot)
	}
}
