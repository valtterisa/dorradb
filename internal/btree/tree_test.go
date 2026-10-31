package btree

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"

	"dorradb/internal/storage"
	"dorradb/internal/storage/page"
	"dorradb/internal/vfs"
)

const all = ^uint64(0) // horizon with no readers: keep only the newest version

func openTree(t *testing.T, cache int) (*Tree, *storage.BufferPool, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "t.dorradb")
	p, err := storage.Open(vfs.OS(), path, true, storage.OpenOptions{CachePages: cache})
	if err != nil {
		t.Fatal(err)
	}
	tr, err := Open(p)
	if err != nil {
		p.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return tr, p, path
}

func mustGet(t *testing.T, tr *Tree, key string, snap uint64) (string, bool) {
	t.Helper()
	v, ok, err := tr.GetVisible([]byte(key), snap)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	return string(v), ok
}

func TestPutGet(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	if err := tr.Put([]byte("a"), []byte("1"), 1, all); err != nil {
		t.Fatal(err)
	}
	if v, ok := mustGet(t, tr, "a", 1); !ok || v != "1" {
		t.Fatalf("got %q ok=%v", v, ok)
	}
}

func TestMVCCVisible(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	_ = tr.Put([]byte("k"), []byte("v1"), 1, 0)
	_ = tr.Put([]byte("k"), []byte("v3"), 3, 1)
	if v, ok := mustGet(t, tr, "k", 2); !ok || v != "v1" {
		t.Fatalf("snap2=%q ok=%v", v, ok)
	}
	if v, ok := mustGet(t, tr, "k", 3); !ok || v != "v3" {
		t.Fatalf("snap3=%q ok=%v", v, ok)
	}
	_ = tr.Delete([]byte("k"), 4, 1)
	if _, ok := mustGet(t, tr, "k", 4); ok {
		t.Fatal("expected missing after delete")
	}
	if v, ok := mustGet(t, tr, "k", 3); !ok || v != "v3" {
		t.Fatalf("old snapshot lost: %q ok=%v", v, ok)
	}
}

func TestHorizonPrunesOnWrite(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	_ = tr.Put([]byte("k"), []byte("v1"), 1, 0)
	_ = tr.Put([]byte("k"), []byte("v2"), 2, 1)
	_ = tr.Put([]byte("k"), []byte("v3"), 3, 2) // reader at 2 only: v1 can go
	if _, ok := mustGet(t, tr, "k", 1); ok {
		t.Fatal("v1 should have been pruned")
	}
	if v, ok := mustGet(t, tr, "k", 2); !ok || v != "v2" {
		t.Fatalf("snap2=%q ok=%v", v, ok)
	}
}

func TestHotKeyManyUpdates(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	v := bytes.Repeat([]byte("x"), 1500)
	for i := uint64(1); i <= 200; i++ {
		// A reader pinned at 1 forces every version to stay live.
		if err := tr.Put([]byte("k"), v, i, 1); err != nil {
			if i > 50 && err == ErrTooBig {
				return // documented limit: too many live versions
			}
			t.Fatalf("update %d: %v", i, err)
		}
	}
}

func TestIgnoreOlderSequence(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	_ = tr.Put([]byte("k"), []byte("new"), 5, all)
	_ = tr.Put([]byte("k"), []byte("old"), 3, all)
	if v, ok := mustGet(t, tr, "k", 10); !ok || v != "new" {
		t.Fatalf("got %q", v)
	}
}

func TestSplitHeight(t *testing.T) {
	tr, p, _ := openTree(t, 64)
	for i := 0; i < 2000; i++ {
		k := []byte(fmt.Sprintf("key-%05d", i))
		if err := tr.Put(k, k, uint64(i+1), all); err != nil {
			t.Fatalf("i=%d: %v", i, err)
		}
	}
	buf, err := p.Get(tr.Root())
	if err != nil {
		t.Fatal(err)
	}
	rootType := TypeOf(buf)
	p.Unpin(tr.Root(), false)
	if rootType != page.TypeBTreeInternal {
		t.Fatalf("expected internal root, got type=%d", rootType)
	}
	checkTree(t, tr)
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("key-%05d", i)
		if v, ok := mustGet(t, tr, k, all); !ok || v != k {
			t.Fatalf("%s got %q ok=%v", k, v, ok)
		}
	}
}

func TestLargeInlineCellsSplit(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	v := bytes.Repeat([]byte("y"), 1990)
	for i := 0; i < 50; i++ {
		if err := tr.Put([]byte{byte(i)}, v, uint64(i+1), all); err != nil {
			t.Fatalf("i=%d: %v", i, err)
		}
	}
	checkTree(t, tr)
}

func TestBigKeysForceInternalSplits(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	for i := 0; i < 300; i++ {
		k := append(bytes.Repeat([]byte("k"), 1000), []byte(fmt.Sprintf("%05d", i))...)
		if err := tr.Put(k, []byte("v"), uint64(i+1), all); err != nil {
			t.Fatalf("i=%d: %v", i, err)
		}
	}
	checkTree(t, tr)
}

func TestOverflowValue(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	big := make([]byte, 100_000)
	for i := range big {
		big[i] = byte(i)
	}
	if err := tr.Put([]byte("big"), big, 1, all); err != nil {
		t.Fatal(err)
	}
	got, ok, err := tr.GetVisible([]byte("big"), 1)
	if err != nil || !ok || !bytes.Equal(got, big) {
		t.Fatalf("ok=%v err=%v len=%d", ok, err, len(got))
	}
}

func TestSeekOrderAcrossEmptyLeaves(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	for i := 0; i < 1000; i++ {
		_ = tr.Put([]byte(fmt.Sprintf("%04d", i)), []byte("v"), uint64(i+1), all)
	}
	// Empty out a middle range so some leaves become empty.
	for i := 100; i < 900; i++ {
		_ = tr.Delete([]byte(fmt.Sprintf("%04d", i)), uint64(2000+i), all)
	}
	if err := tr.Prune(all, true); err != nil {
		t.Fatal(err)
	}
	it, err := tr.Seek([]byte("0050"))
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for ; it.Valid(); _ = it.Next() {
		k, _ := it.Key()
		keys = append(keys, string(k))
	}
	it.Close()
	if len(keys) != 150 || keys[0] != "0050" || keys[49] != "0099" || keys[50] != "0900" {
		t.Fatalf("got %d keys: first=%v", len(keys), keys[:3])
	}
}

func TestPruneForgetDeleted(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	_ = tr.Put([]byte("a"), []byte("1"), 1, 0)
	_ = tr.Put([]byte("a"), []byte("2"), 2, 0)
	_ = tr.Put([]byte("b"), []byte("x"), 3, 0)
	_ = tr.Delete([]byte("b"), 4, 0)
	if err := tr.Prune(4, true); err != nil {
		t.Fatal(err)
	}
	if v, ok := mustGet(t, tr, "a", 4); !ok || v != "2" {
		t.Fatalf("a=%q ok=%v", v, ok)
	}
	if _, ok := mustGet(t, tr, "a", 1); ok {
		t.Fatal("a@1 should be pruned")
	}
	if tr.LiveCount() != 1 {
		t.Fatalf("live=%d", tr.LiveCount())
	}
}

func TestPruneKeepsVersionsNewerThanHorizon(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	_ = tr.Put([]byte("a"), []byte("1"), 5, 0)
	if err := tr.Prune(3, true); err != nil {
		t.Fatal(err)
	}
	if v, ok := mustGet(t, tr, "a", 5); !ok || v != "1" {
		t.Fatalf("key newer than horizon was dropped: %q ok=%v", v, ok)
	}
}

// TestRandomAgainstMap mirrors random ops into a Go map with a tiny cache
// (forcing many flushes) and checks every read, the sorted scan, and the
// structural invariants, including after reopening the file.
func TestRandomAgainstMap(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			tr, p, path := openTree(t, 16)
			model := map[string]string{}
			seq := uint64(0)
			for op := 0; op < 4000; op++ {
				k := fmt.Sprintf("k%04d", rng.Intn(800))
				seq++
				switch r := rng.Intn(10); {
				case r < 6:
					n := rng.Intn(300)
					if rng.Intn(50) == 0 {
						n = 5000 + rng.Intn(20000) // overflow value
					}
					v := fmt.Sprintf("%d:", seq) + string(bytes.Repeat([]byte{byte('a' + r)}, n))
					if err := tr.Put([]byte(k), []byte(v), seq, all); err != nil {
						t.Fatalf("op %d put: %v", op, err)
					}
					model[k] = v
				case r < 8:
					if err := tr.Delete([]byte(k), seq, all); err != nil {
						t.Fatalf("op %d delete: %v", op, err)
					}
					delete(model, k)
				default:
					got, ok := mustGet(t, tr, k, all)
					want, wok := model[k]
					if ok != wok || got != want {
						t.Fatalf("op %d get %s: ok=%v want ok=%v", op, k, ok, wok)
					}
				}
				if op%1000 == 999 {
					if err := tr.Prune(all, true); err != nil {
						t.Fatal(err)
					}
				}
			}
			verifyAgainst(t, tr, model)

			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			p2, err := storage.Open(vfs.OS(), path, false, storage.OpenOptions{CachePages: 16})
			if err != nil {
				t.Fatal(err)
			}
			defer p2.Close()
			tr2, err := Open(p2)
			if err != nil {
				t.Fatal(err)
			}
			verifyAgainst(t, tr2, model)
		})
	}
}

func verifyAgainst(t *testing.T, tr *Tree, model map[string]string) {
	t.Helper()
	checkTree(t, tr)
	want := make([]string, 0, len(model))
	for k := range model {
		want = append(want, k)
	}
	sort.Strings(want)
	var got []string
	it, err := tr.Seek(nil)
	if err != nil {
		t.Fatal(err)
	}
	for ; it.Valid(); _ = it.Next() {
		c, err := it.Cell()
		if err != nil {
			t.Fatal(err)
		}
		if !c.Versions[0].Deleted {
			got = append(got, string(c.Key))
		}
	}
	it.Close()
	if len(got) != len(want) {
		t.Fatalf("scan has %d live keys, model %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("scan[%d]=%s want %s", i, got[i], want[i])
		}
		if v, ok := mustGet(t, tr, want[i], all); !ok || v != model[want[i]] {
			t.Fatalf("get %s mismatch (ok=%v)", want[i], ok)
		}
	}
}

// checkTree verifies: keys sorted within and across leaves, every key inside
// its parent's separator bounds, all leaves at one depth, and the sibling
// chain visiting exactly the leaves reachable from the root, in order.
func checkTree(t *testing.T, tr *Tree) {
	t.Helper()
	var leaves []uint32
	leafDepth := -1
	var walk func(id uint32, lo, hi []byte, depth int)
	walk = func(id uint32, lo, hi []byte, depth int) {
		buf, err := tr.pool.Get(id)
		if err != nil {
			t.Fatalf("page %d: %v", id, err)
		}
		defer tr.pool.Unpin(id, false)
		inBounds := func(k []byte) bool {
			return (lo == nil || bytes.Compare(k, lo) >= 0) && (hi == nil || bytes.Compare(k, hi) < 0)
		}
		switch TypeOf(buf) {
		case page.TypeBTreeLeaf:
			if leafDepth == -1 {
				leafDepth = depth
			} else if depth != leafDepth {
				t.Fatalf("leaf %d at depth %d, want %d", id, depth, leafDepth)
			}
			leaves = append(leaves, id)
			var prev []byte
			for i := 0; i < NKeys(buf); i++ {
				k, _ := keyAt(buf, i)
				if prev != nil && bytes.Compare(prev, k) >= 0 {
					t.Fatalf("leaf %d keys out of order", id)
				}
				if !inBounds(k) {
					t.Fatalf("leaf %d key %q outside [%q,%q)", id, k, lo, hi)
				}
				prev = append(prev[:0], k...)
			}
		case page.TypeBTreeInternal:
			left, seps, err := sepsFromInternal(buf)
			if err != nil {
				t.Fatal(err)
			}
			childLo := lo
			child := left
			for _, s := range seps {
				if !inBounds(s.Key) {
					t.Fatalf("internal %d sep %q outside bounds", id, s.Key)
				}
				walk(child, childLo, s.Key, depth+1)
				childLo, child = s.Key, s.Child
			}
			walk(child, childLo, hi, depth+1)
		default:
			t.Fatalf("page %d has type %d", id, TypeOf(buf))
		}
	}
	walk(tr.Root(), nil, nil, 0)

	id, err := tr.leftmostLeaf()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; id != 0; i++ {
		if i >= len(leaves) || leaves[i] != id {
			t.Fatalf("sibling chain diverges at leaf #%d (page %d)", i, id)
		}
		buf, err := tr.pool.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		next := RightSibling(buf)
		tr.pool.Unpin(id, false)
		id = next
		if next == 0 && i != len(leaves)-1 {
			t.Fatalf("sibling chain ends after %d of %d leaves", i+1, len(leaves))
		}
	}
}
