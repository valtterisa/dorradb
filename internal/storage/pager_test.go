package storage

import (
	"path/filepath"
	"testing"

	"dorradb/internal/encode"
	"dorradb/internal/index"
	"dorradb/internal/storage/page"
	"dorradb/internal/vfs"
)

func TestPagerGetDirtyFlush(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dorradb")
	s, err := OpenStore(vfs.OS(), path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id, buf, err := s.pager.NewPage(page.InitHeap)
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
	s.pager.Unpin(id, true)
	if err := s.pager.FlushAll(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenStore(vfs.OS(), path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	buf2, err := s2.pager.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	rec, _, err := page.ReadSlot(buf2, 0)
	s2.pager.Unpin(id, false)
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
	s, err := OpenStore(vfs.OS(), path, true)
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
	s, err := OpenStore(vfs.OS(), path, true)
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
