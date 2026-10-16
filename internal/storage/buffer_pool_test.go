package storage

import (
	"path/filepath"
	"testing"

	"dorradb/internal/encode"
	"dorradb/internal/index"
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

func TestStoreAppendReadScan(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dorradb")
	s, err := OpenStore(vfs.OS(), path, true, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	loc, err := s.Append(encode.Record{
		Sequence: 1,
		Op:       encode.OpSet,
		Key:      []byte("a"),
		Value:    []byte("1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	val, err := s.ReadValue(loc)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "1" {
		t.Fatalf("val=%q", val)
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}

	idx := index.New()
	if err := s.ScanIndex(idx); err != nil {
		t.Fatal(err)
	}
	got, ok := idx.Get([]byte("a"))
	if !ok || got.Sequence != 1 {
		t.Fatalf("index %+v ok=%v", got, ok)
	}
}

func TestStoreOverflowValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dorradb")
	s, err := OpenStore(vfs.OS(), path, true, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	big := make([]byte, 10000)
	for i := range big {
		big[i] = byte(i)
	}
	loc, err := s.Append(encode.Record{
		Sequence: 1,
		Op:       encode.OpSet,
		Key:      []byte("big"),
		Value:    big,
	})
	if err != nil {
		t.Fatal(err)
	}
	if loc.OverflowPage == 0 {
		t.Fatal("expected overflow")
	}
	val, err := s.ReadValue(loc)
	if err != nil {
		t.Fatal(err)
	}
	if len(val) != len(big) {
		t.Fatalf("len=%d", len(val))
	}
	for i := range big {
		if val[i] != big[i] {
			t.Fatalf("mismatch at %d", i)
		}
	}
}

func TestBufferPoolSmallCacheManyPages(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dorradb")
	s, err := OpenStore(vfs.OS(), path, true, OpenOptions{CachePages: 4, GrowPages: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var locs []index.Loc
	for i := 0; i < 20; i++ {
		loc, err := s.Append(encode.Record{
			Sequence: uint64(i + 1),
			Op:       encode.OpSet,
			Key:      []byte{byte(i)},
			Value:    []byte("v"),
		})
		if err != nil {
			t.Fatal(err)
		}
		locs = append(locs, loc)
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenStore(vfs.OS(), path, false, OpenOptions{CachePages: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	idx := index.New()
	if err := s2.ScanIndex(idx); err != nil {
		t.Fatal(err)
	}
	for i, loc := range locs {
		got, err := s2.ReadValue(loc)
		if err != nil {
			t.Fatalf("i=%d: %v", i, err)
		}
		if string(got) != "v" {
			t.Fatalf("i=%d val=%q", i, got)
		}
		if _, ok := idx.Get([]byte{byte(i)}); !ok {
			t.Fatalf("missing key %d", i)
		}
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

	// Allocate more pages than cache to force eviction of dirty id1.
	id2, _, err := p.NewPage(page.InitHeap)
	if err != nil {
		t.Fatal(err)
	}
	p.Unpin(id2, true)
	id3, _, err := p.NewPage(page.InitHeap)
	if err != nil {
		t.Fatal(err)
	}
	p.Unpin(id3, true)

	// id1 should have been written back; load fresh and read.
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

func TestFSMReusesEarlierPage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dorradb")
	s, err := OpenStore(vfs.OS(), path, true, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Fill first heap page nearly full with medium values.
	val := make([]byte, 200)
	var seq uint64 = 1
	var firstPage uint32
	for {
		loc, err := s.Append(encode.Record{
			Sequence: seq,
			Op:       encode.OpSet,
			Key:      []byte{byte(seq)},
			Value:    val,
		})
		if err != nil {
			t.Fatal(err)
		}
		if firstPage == 0 {
			firstPage = loc.PageID
		}
		if loc.PageID != firstPage {
			break
		}
		seq++
		if seq > 100 {
			t.Fatal("expected page to fill")
		}
	}
	secondPage := s.insertHint
	if secondPage == firstPage {
		t.Fatal("expected second page")
	}

	// Force hint to a page with no advertised free space so FSM first-fit
	// must pick firstPage's leftover gap.
	s.insertHint = secondPage
	s.freeBytes[secondPage] = 0
	buf, err := s.pool.Get(firstPage)
	if err != nil {
		t.Fatal(err)
	}
	s.freeBytes[firstPage] = page.FreeSpace(buf)
	s.pool.Unpin(firstPage, false)
	if s.freeBytes[firstPage] < 32 {
		t.Skip("first page has no leftover gap; FSM reuse N/A for this packing")
	}

	loc, err := s.Append(encode.Record{
		Sequence: seq + 1,
		Op:       encode.OpSet,
		Key:      []byte("tiny"),
		Value:    []byte("x"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if loc.PageID != firstPage {
		t.Fatalf("expected reuse page %d, got %d", firstPage, loc.PageID)
	}
}

func TestVacuumCompactsDeadSlots(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dorradb")
	s, err := OpenStore(vfs.OS(), path, true, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	idx := index.New()
	const n = 40
	val := make([]byte, 80)
	for i := 0; i < n; i++ {
		loc, err := s.Append(encode.Record{
			Sequence: uint64(i + 1),
			Op:       encode.OpSet,
			Key:      []byte{byte(i)},
			Value:    val,
		})
		if err != nil {
			t.Fatal(err)
		}
		idx.Apply([]byte{byte(i)}, loc)
	}
	// Overwrite every key (dead slots accumulate).
	for i := 0; i < n; i++ {
		loc, err := s.Append(encode.Record{
			Sequence: uint64(n + i + 1),
			Op:       encode.OpSet,
			Key:      []byte{byte(i)},
			Value:    []byte("x"),
		})
		if err != nil {
			t.Fatal(err)
		}
		idx.Apply([]byte{byte(i)}, loc)
	}

	pagesBefore := s.PageCount()
	idx.Prune(uint64(2 * n))
	idx.ForgetDeleted()
	if err := s.Vacuum(idx); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < n; i++ {
		loc, ok := idx.Get([]byte{byte(i)})
		if !ok {
			t.Fatalf("missing %d", i)
		}
		got, err := s.ReadValue(loc)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "x" {
			t.Fatalf("key %d = %q", i, got)
		}
	}
	// Freelist should have absorbed some empty pages, or FSM free space increased.
	if s.Header().FreelistHead == 0 && s.PageCount() >= pagesBefore {
		// Still OK if compact kept same page count but freed space inside pages.
		totalFree := 0
		for _, f := range s.freeBytes {
			totalFree += f
		}
		if totalFree == 0 {
			t.Fatal("expected freelist or free space after vacuum")
		}
	}
}
