package index

import "testing"

func TestGetVisibleMVCC(t *testing.T) {
	idx := New()
	idx.Apply([]byte("k"), Loc{PageID: 1, Slot: 0, Sequence: 1})
	idx.Apply([]byte("k"), Loc{PageID: 1, Slot: 1, Sequence: 3})
	loc, ok := idx.GetVisible([]byte("k"), 2)
	if !ok || loc.Sequence != 1 || loc.Slot != 0 {
		t.Fatalf("got %+v ok=%v", loc, ok)
	}
	loc, ok = idx.GetVisible([]byte("k"), 3)
	if !ok || loc.Sequence != 3 {
		t.Fatalf("got %+v ok=%v", loc, ok)
	}
	idx.Apply([]byte("k"), Loc{Sequence: 4, Deleted: true})
	if _, ok := idx.GetVisible([]byte("k"), 4); ok {
		t.Fatal("expected missing after delete")
	}
}

func TestRemapAndForgetDeleted(t *testing.T) {
	idx := New()
	idx.Apply([]byte("a"), Loc{PageID: 2, Slot: 5, Sequence: 1})
	idx.RemapSlots(2, map[uint16]uint16{5: 0})
	loc, ok := idx.Get([]byte("a"))
	if !ok || loc.Slot != 0 {
		t.Fatalf("got %+v", loc)
	}
	idx.Apply([]byte("a"), Loc{Sequence: 2, Deleted: true})
	idx.ForgetDeleted()
	if _, ok := idx.Get([]byte("a")); ok {
		t.Fatal("expected forgotten")
	}
}
