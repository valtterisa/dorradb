package page

import (
	"bytes"
	"testing"

	"dorradb/internal/encode"
)

func TestHeapInsertRead(t *testing.T) {
	buf := make([]byte, Size)
	InitHeap(buf)
	r := Record{
		Sequence: 1,
		Op:       encode.OpSet,
		Key:      []byte("hello"),
		Value:    []byte("world"),
		ValueLen: 5,
	}
	slot, err := Insert(buf, r)
	if err != nil {
		t.Fatal(err)
	}
	if slot != 0 {
		t.Fatalf("slot=%d", slot)
	}
	got, _, err := ReadSlot(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.Sequence != 1 || string(got.Key) != "hello" || string(got.Value) != "world" {
		t.Fatalf("got %+v", got)
	}
}

func TestHeapOverflowRecord(t *testing.T) {
	buf := make([]byte, Size)
	InitHeap(buf)
	r := Record{
		Sequence:     2,
		Op:           encode.OpSet,
		Key:          []byte("k"),
		OverflowPage: 7,
		ValueLen:     100000,
	}
	slot, err := Insert(buf, r)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := ReadSlot(buf, slot)
	if err != nil {
		t.Fatal(err)
	}
	if got.OverflowPage != 7 || got.ValueLen != 100000 {
		t.Fatalf("got %+v", got)
	}
}

func TestOverflowChunk(t *testing.T) {
	buf := make([]byte, Size)
	data := bytes.Repeat([]byte("a"), 100)
	if err := WriteOverflowChunk(buf, 3, data); err != nil {
		t.Fatal(err)
	}
	next, out, err := ReadOverflowChunk(buf)
	if err != nil {
		t.Fatal(err)
	}
	if next != 3 || !bytes.Equal(out, data) {
		t.Fatalf("next=%d out=%q", next, out)
	}
}

func TestMetaPage(t *testing.T) {
	h := encode.Header{
		Sequence:            9,
		WalCheckpointOffset: encode.HeaderSize,
		PageSize:            Size,
		PageCount:           3,
	}
	buf := EncodeMetaPage(h)
	got, err := DecodeMetaPage(buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Sequence != 9 || got.PageCount != 3 || got.PageSize != Size {
		t.Fatalf("got %+v", got)
	}
	if buf[encode.HeaderSize] != TypeMeta {
		t.Fatalf("meta marker=%d", buf[encode.HeaderSize])
	}
}

func TestPageFull(t *testing.T) {
	buf := make([]byte, Size)
	InitHeap(buf)
	big := bytes.Repeat([]byte("x"), 3000)
	r := Record{Sequence: 1, Op: encode.OpSet, Key: []byte("a"), Value: big, ValueLen: uint32(len(big))}
	if _, err := Insert(buf, r); err != nil {
		t.Fatal(err)
	}
	r2 := Record{Sequence: 2, Op: encode.OpSet, Key: []byte("b"), Value: big, ValueLen: uint32(len(big))}
	if _, err := Insert(buf, r2); err != ErrPageFull {
		t.Fatalf("want ErrPageFull, got %v", err)
	}
}
