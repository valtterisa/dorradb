// Test harness for the B+ tree the engine uses.
//
// Fill in tree.go until `go test ./internal/btree` passes, then run
// `go test ./...` for the end-to-end check through the engine.
//
// Semantics the engine relies on:
//   - Every key holds a chain of versions, newest first. Each version has a
//     sequence number and is either a value or a tombstone (Delete).
//   - GetVisible(key, snap) returns the newest version with sequence <= snap;
//     a tombstone there means "not found".
//   - Put/Delete with a sequence <= the key's newest sequence is a no-op (WAL
//     replay writes the same changes again after a crash).
//   - horizon = oldest snapshot still in use. Versions no snapshot >= horizon
//     can see may be dropped. horizon = ^uint64(0) means "keep only newest".
//   - Values can be bigger than a page (4 KiB) -> overflow pages.
//   - The buffer pool never evicts dirty pages by itself. When it runs low on
//     free frames, the tree must call the flusher (pool.FlushAll by default,
//     replaced via SetFlusher) at a point where the tree is consistent.
//   - Whenever the root page changes (root split), call onRoot(newRoot). Open
//     stores the root in the pool header's TableRoot instead.
//   - Many readers may run while one writer writes.
package btree_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"dorradb/internal/btree"
	"dorradb/internal/storage"
	"dorradb/internal/vfs"
)

// API contract: this block fails to compile until your package matches what
// the engine calls.
var (
	_ func(*storage.BufferPool) (*btree.Tree, error)                             = btree.Open
	_ func(*storage.BufferPool, uint32, func(uint32) error) *btree.Tree          = btree.Attach
	_ func(*storage.BufferPool, func(uint32) error) (*btree.Tree, uint32, error) = btree.NewEmpty

	_ func(*btree.Tree, []byte, []byte, uint64, uint64) error = (*btree.Tree).Put
	_ func(*btree.Tree, []byte, uint64, uint64) error         = (*btree.Tree).Delete
	_ func(*btree.Tree, []byte, uint64) ([]byte, bool, error) = (*btree.Tree).GetVisible
	_ func(*btree.Tree, []byte) (*btree.Iterator, error)      = (*btree.Tree).Seek
	_ func(*btree.Tree, uint64, bool) error                   = (*btree.Tree).Prune
	_ func(*btree.Tree) int                                   = (*btree.Tree).LiveCount
	_ func(*btree.Tree) int64                                 = (*btree.Tree).ApproxLiveBytes
	_ func(*btree.Tree) uint32                                = (*btree.Tree).Root
	_ func(*btree.Tree, func() error)                         = (*btree.Tree).SetFlusher
	_ func(*btree.Iterator) bool                              = (*btree.Iterator).Valid
	_ func(*btree.Iterator) error                             = (*btree.Iterator).Err
	_ func(*btree.Iterator) ([]byte, error)                   = (*btree.Iterator).Key
	_ func(*btree.Iterator, uint64) ([]byte, bool, error)     = (*btree.Iterator).VisibleValue
	_ func(*btree.Iterator) error                             = (*btree.Iterator).Next
	_ func(*btree.Iterator)                                   = (*btree.Iterator).Close
)

const all = ^uint64(0) // horizon with no readers: keep only the newest version

func openPool(t *testing.T, path string, cache int) *storage.BufferPool {
	t.Helper()
	p, err := storage.Open(vfs.OS(), path, true, storage.OpenOptions{CachePages: cache})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func openTree(t *testing.T, cache int) (*btree.Tree, *storage.BufferPool, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "t.dorradb")
	p := openPool(t, path, cache)
	tr, err := btree.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	return tr, p, path
}

func put(t *testing.T, tr *btree.Tree, k, v string, seq, horizon uint64) {
	t.Helper()
	if err := tr.Put([]byte(k), []byte(v), seq, horizon); err != nil {
		t.Fatalf("put %q@%d: %v", k, seq, err)
	}
}

func del(t *testing.T, tr *btree.Tree, k string, seq, horizon uint64) {
	t.Helper()
	if err := tr.Delete([]byte(k), seq, horizon); err != nil {
		t.Fatalf("delete %q@%d: %v", k, seq, err)
	}
}

func expect(t *testing.T, tr *btree.Tree, k string, snap uint64, want string, wantOK bool) {
	t.Helper()
	v, ok, err := tr.GetVisible([]byte(k), snap)
	if err != nil {
		t.Fatalf("get %q@%d: %v", k, snap, err)
	}
	if ok != wantOK || (ok && string(v) != want) {
		t.Fatalf("get %q@%d = %.40q ok=%v, want %.40q ok=%v", k, snap, v, ok, want, wantOK)
	}
}

// scan returns every key/value visible at snap, the way engine.Scanner does.
func scan(t *testing.T, tr *btree.Tree, start []byte, snap uint64) (keys, vals []string) {
	t.Helper()
	it, err := tr.Seek(start)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	for it.Valid() {
		k, err := it.Key()
		if err != nil {
			t.Fatal(err)
		}
		v, ok, err := it.VisibleValue(snap)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			keys = append(keys, string(k))
			vals = append(vals, string(v))
		}
		if err := it.Next(); err != nil {
			t.Fatal(err)
		}
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return keys, vals
}

func key(i int) string { return fmt.Sprintf("key-%06d", i) }

func TestEmptyTree(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	expect(t, tr, "nope", all, "", false)
	if keys, _ := scan(t, tr, nil, all); len(keys) != 0 {
		t.Fatalf("scan of empty tree: %v", keys)
	}
	if tr.LiveCount() != 0 || tr.ApproxLiveBytes() != 0 {
		t.Fatal("empty tree should have no live keys")
	}
}

func TestPutGetOverwrite(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	put(t, tr, "a", "1", 1, all)
	put(t, tr, "b", "2", 2, all)
	put(t, tr, "", "empty key", 3, all)
	put(t, tr, "c", "", 4, all)
	expect(t, tr, "a", all, "1", true)
	expect(t, tr, "b", all, "2", true)
	expect(t, tr, "", all, "empty key", true)
	expect(t, tr, "c", all, "", true)
	expect(t, tr, "missing", all, "", false)

	put(t, tr, "a", "1b", 5, all)
	expect(t, tr, "a", all, "1b", true)

	// The tree must not keep a reference to the caller's slices.
	k, v := []byte("mut"), []byte("orig")
	if err := tr.Put(k, v, 6, all); err != nil {
		t.Fatal(err)
	}
	k[0], v[0] = 'X', 'X'
	expect(t, tr, "mut", all, "orig", true)
}

func TestDelete(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	put(t, tr, "a", "1", 1, all)
	del(t, tr, "a", 2, all)
	expect(t, tr, "a", all, "", false)
	del(t, tr, "never-existed", 3, all) // must not fail
	expect(t, tr, "never-existed", all, "", false)
	put(t, tr, "a", "again", 4, all)
	expect(t, tr, "a", all, "again", true)
}

// WAL replay re-applies writes the tree may already contain.
func TestReplayIsNoOp(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	put(t, tr, "k", "new", 5, all)
	put(t, tr, "k", "old", 3, all)
	put(t, tr, "k", "same", 5, all)
	del(t, tr, "k", 4, all)
	expect(t, tr, "k", all, "new", true)
}

// Table-driven cases: apply ops in order, then check reads.
// op: "put" or "del". horizon 0 keeps every version, all keeps only the newest.
type op struct {
	kind         string
	key, val     string
	seq, horizon uint64
}

type read struct {
	key  string
	snap uint64
	want string
	ok   bool
}

var cases = []struct {
	name  string
	ops   []op
	reads []read
}{
	{
		name:  "single put",
		ops:   []op{{"put", "a", "1", 1, all}},
		reads: []read{{"a", all, "1", true}, {"b", all, "", false}},
	},
	{
		name: "overwrite keeps newest",
		ops: []op{
			{"put", "a", "1", 1, all},
			{"put", "a", "2", 2, all},
			{"put", "a", "3", 3, all},
		},
		reads: []read{{"a", all, "3", true}},
	},
	{
		name: "delete then reinsert",
		ops: []op{
			{"put", "a", "1", 1, all},
			{"del", "a", "", 2, all},
			{"put", "a", "back", 3, all},
		},
		reads: []read{{"a", all, "back", true}},
	},
	{
		name: "old sequence ignored",
		ops: []op{
			{"put", "a", "new", 10, all},
			{"put", "a", "stale", 9, all},
			{"del", "a", "", 10, all},
		},
		reads: []read{{"a", all, "new", true}},
	},
	{
		name: "snapshot reads",
		ops: []op{
			{"put", "x", "v1", 1, 0},
			{"put", "x", "v2", 2, 0},
			{"del", "x", "", 3, 0},
		},
		reads: []read{
			{"x", 0, "", false},
			{"x", 1, "v1", true},
			{"x", 2, "v2", true},
			{"x", 3, "", false},
			{"x", all, "", false},
		},
	},
	{
		name: "keys are independent",
		ops: []op{
			{"put", "apple", "red", 1, all},
			{"put", "banana", "yellow", 2, all},
			{"put", "app", "short", 3, all},
			{"del", "banana", "", 4, 0}, // horizon 0: keep history
		},
		reads: []read{
			{"apple", all, "red", true},
			{"app", all, "short", true},
			{"banana", all, "", false},
			{"banana", 3, "yellow", true},
			{"ap", all, "", false},
		},
	},
	{
		name: "binary keys and values",
		ops: []op{
			{"put", "\x00", "zero", 1, all},
			{"put", "\xff\xff", "\x00\x01\x02", 2, all},
		},
		reads: []read{{"\x00", all, "zero", true}, {"\xff\xff", all, "\x00\x01\x02", true}},
	},
}

func TestCases(t *testing.T) {
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tr, _, _ := openTree(t, 64)
			for _, o := range c.ops {
				if o.kind == "put" {
					put(t, tr, o.key, o.val, o.seq, o.horizon)
				} else {
					del(t, tr, o.key, o.seq, o.horizon)
				}
			}
			for _, r := range c.reads {
				expect(t, tr, r.key, r.snap, r.want, r.ok)
			}
		})
	}
}

// Scan order is plain byte order (bytes.Compare).
func TestScanOrder(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	input := []string{"m", "a", "z", "ab", "", "b", "aa", "\x00", "Z"}
	for i, k := range input {
		put(t, tr, k, "v"+k, uint64(i+1), all)
	}
	want := []string{"", "\x00", "Z", "a", "aa", "ab", "b", "m", "z"}
	keys, vals := scan(t, tr, nil, all)
	if fmt.Sprintf("%q", keys) != fmt.Sprintf("%q", want) {
		t.Fatalf("scan order\n got %q\nwant %q", keys, want)
	}
	for i := range keys {
		if vals[i] != "v"+keys[i] {
			t.Fatalf("key %q has value %q", keys[i], vals[i])
		}
	}
	keys, _ = scan(t, tr, []byte("ab"), all)
	if fmt.Sprintf("%q", keys) != fmt.Sprintf("%q", []string{"ab", "b", "m", "z"}) {
		t.Fatalf("seek(ab) = %q", keys)
	}
}

func TestSnapshots(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	// horizon 0: a reader at snapshot 0 exists, so every version is kept.
	put(t, tr, "k", "v1", 1, 0)
	put(t, tr, "k", "v3", 3, 0)
	del(t, tr, "k", 5, 0)
	put(t, tr, "k", "v7", 7, 0)

	expect(t, tr, "k", 0, "", false) // before the key existed
	expect(t, tr, "k", 1, "v1", true)
	expect(t, tr, "k", 2, "v1", true)
	expect(t, tr, "k", 4, "v3", true)
	expect(t, tr, "k", 5, "", false) // deleted
	expect(t, tr, "k", 6, "", false)
	expect(t, tr, "k", 100, "v7", true)

	_, vals := scan(t, tr, nil, 4)
	if len(vals) != 1 || vals[0] != "v3" {
		t.Fatalf("scan@4 = %v", vals)
	}
	if keys, _ := scan(t, tr, nil, 5); len(keys) != 0 {
		t.Fatalf("scan@5 = %v, want nothing", keys)
	}
}

// A hot key rewritten many times with no old readers must not grow forever.
func TestHorizonDropsOldVersions(t *testing.T) {
	tr, p, _ := openTree(t, 64)
	big := string(bytes.Repeat([]byte("x"), 1000))
	for i := 1; i <= 2000; i++ {
		put(t, tr, "hot", fmt.Sprint(i, big), uint64(i), all)
	}
	expect(t, tr, "hot", all, fmt.Sprint(2000, big), true)
	if n := p.PageCount(); n > 50 {
		t.Fatalf("file grew to %d pages for one key; old versions are not dropped/freed", n)
	}
}

func TestManyKeysSplit(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	firstRoot := tr.Root()
	const n = 5000
	for _, i := range rand.New(rand.NewSource(1)).Perm(n) {
		put(t, tr, key(i), "v"+key(i), uint64(i+1), all)
	}
	if tr.Root() == firstRoot {
		t.Fatal("root never changed; the tree did not split")
	}
	for i := 0; i < n; i++ {
		expect(t, tr, key(i), all, "v"+key(i), true)
	}
	if tr.LiveCount() != n {
		t.Fatalf("LiveCount = %d, want %d", tr.LiveCount(), n)
	}
}

func TestBigKeysAndValues(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	seq := uint64(0)
	model := map[string]string{}
	for i := 0; i < 200; i++ {
		// ~500 byte keys force deep internal splits with few separators per page.
		k := fmt.Sprintf("%04d", i) + string(bytes.Repeat([]byte("k"), 500))
		var v string
		switch i % 3 {
		case 0:
			v = string(bytes.Repeat([]byte{byte('a' + i%26)}, 1500)) // big inline
		case 1:
			v = string(bytes.Repeat([]byte{byte('a' + i%26)}, 20000)) // overflow
		default:
			v = "small"
		}
		seq++
		put(t, tr, k, v, seq, all)
		model[k] = v
	}
	for k, v := range model {
		expect(t, tr, k, all, v, true)
	}
	keys, vals := scan(t, tr, nil, all)
	if len(keys) != len(model) {
		t.Fatalf("scan found %d keys, want %d", len(keys), len(model))
	}
	for i := range keys {
		if model[keys[i]] != vals[i] {
			t.Fatalf("scan value mismatch at %d", i)
		}
	}
}

func TestSeek(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	for i := 0; i < 3000; i += 2 { // even keys only
		put(t, tr, key(i), key(i), uint64(i+1), all)
	}
	// Delete a long run and prune it away so some leaves end up empty;
	// scans must skip them.
	for i := 1000; i < 2000; i += 2 {
		del(t, tr, key(i), uint64(10000+i), all)
	}
	if err := tr.Prune(all, true); err != nil {
		t.Fatal(err)
	}

	keys, _ := scan(t, tr, nil, all)
	if !sort.StringsAreSorted(keys) {
		t.Fatal("scan is not in key order")
	}
	if len(keys) != 1000 {
		t.Fatalf("scan found %d keys, want 1000", len(keys))
	}

	// Seek to a missing key lands on the next one.
	keys, _ = scan(t, tr, []byte(key(501)), all)
	if len(keys) == 0 || keys[0] != key(502) {
		t.Fatalf("seek(%s) found %d keys, want first %s", key(501), len(keys), key(502))
	}
	// Seek across the deleted range.
	keys, _ = scan(t, tr, []byte(key(1000)), all)
	if len(keys) == 0 || keys[0] != key(2000) {
		t.Fatalf("seek into deleted range found %d keys, want first %s", len(keys), key(2000))
	}
	// Seek past the end.
	if keys, _ = scan(t, tr, []byte("zzz"), all); len(keys) != 0 {
		t.Fatalf("seek past end found %v", keys)
	}
}

func TestPrune(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	for i := 0; i < 1000; i++ {
		put(t, tr, key(i), "abc", uint64(i+1), 0)
	}
	for i := 0; i < 1000; i += 2 {
		del(t, tr, key(i), uint64(2000+i), 0)
	}
	if got := tr.LiveCount(); got != 500 {
		t.Fatalf("LiveCount = %d, want 500", got)
	}
	if got, want := tr.ApproxLiveBytes(), int64(500*(len(key(0))+3)); got != want {
		t.Fatalf("ApproxLiveBytes = %d, want %d", got, want)
	}

	// Keep everything readers at snapshot 2500 can see; forget dead keys
	// deleted at or before it.
	if err := tr.Prune(2500, true); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		expect(t, tr, key(i), all, "abc", i%2 == 1)
	}
	// Deleted after 2500: still visible to a reader at 2500.
	expect(t, tr, key(998), 2500, "abc", true)
	if got := tr.LiveCount(); got != 500 {
		t.Fatalf("LiveCount after prune = %d, want 500", got)
	}

	if err := tr.Prune(all, true); err != nil {
		t.Fatal(err)
	}
	keys, _ := scan(t, tr, nil, all)
	if len(keys) != 500 {
		t.Fatalf("after full prune scan found %d keys, want 500", len(keys))
	}
}

// The pool only frees frames when someone flushes. With a tiny cache the
// tree has to call its flusher, or it fails with storage.ErrNoFreeFrame.
func TestSmallCacheUsesFlusher(t *testing.T) {
	tr, p, _ := openTree(t, 16)
	flushes := 0
	tr.SetFlusher(func() error { flushes++; return p.FlushAll() })
	for i := 0; i < 5000; i++ {
		put(t, tr, key(i), string(bytes.Repeat([]byte("v"), 100)), uint64(i+1), all)
	}
	put(t, tr, "big", string(bytes.Repeat([]byte("b"), 60000)), 6000, all)
	if flushes == 0 {
		t.Fatal("flusher was never called")
	}
	for i := 0; i < 5000; i += 97 {
		expect(t, tr, key(i), all, string(bytes.Repeat([]byte("v"), 100)), true)
	}
	expect(t, tr, "big", all, string(bytes.Repeat([]byte("b"), 60000)), true)
}

// Open stores its root in the header; NewEmpty/Attach trees report root
// changes through onRoot. Several trees share one pool (SQL tables).
func TestReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.dorradb")
	p, err := storage.Open(vfs.OS(), path, true, storage.OpenOptions{CachePages: 32})
	if err != nil {
		t.Fatal(err)
	}
	main, err := btree.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	var tableRoot uint32
	table, root, err := btree.NewEmpty(p, func(r uint32) error { tableRoot = r; return nil })
	if err != nil {
		t.Fatal(err)
	}
	tableRoot = root

	for i := 0; i < 3000; i++ {
		put(t, main, key(i), "main", uint64(i+1), all)
		put(t, table, key(i), "table", uint64(i+1), all)
	}
	if tableRoot != table.Root() {
		t.Fatalf("onRoot last reported %d, but Root() is %d", tableRoot, table.Root())
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	p2 := openPool(t, path, 32)
	main2, err := btree.Open(p2)
	if err != nil {
		t.Fatal(err)
	}
	table2 := btree.Attach(p2, tableRoot, func(r uint32) error { tableRoot = r; return nil })
	for i := 0; i < 3000; i++ {
		expect(t, main2, key(i), all, "main", true)
		expect(t, table2, key(i), all, "table", true)
	}
	// Keep writing after reopen.
	for i := 3000; i < 4000; i++ {
		put(t, table2, key(i), "table", uint64(i+1), all)
	}
	if table2.LiveCount() != 4000 || main2.LiveCount() != 3000 {
		t.Fatalf("counts after reopen: table=%d main=%d", table2.LiveCount(), main2.LiveCount())
	}
}

// One writer, many readers (run with -race).
func TestConcurrentReaders(t *testing.T) {
	tr, _, _ := openTree(t, 64)
	for i := 0; i < 500; i++ {
		put(t, tr, key(i), "v0", uint64(i+1), all)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	errs := make(chan error, 8)
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(r)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, ok, err := tr.GetVisible([]byte(key(rng.Intn(500))), all); err != nil || !ok {
					errs <- fmt.Errorf("reader get: ok=%v err=%v", ok, err)
					return
				}
				it, err := tr.Seek(nil)
				if err != nil {
					errs <- err
					return
				}
				for n := 0; it.Valid() && n < 50; n++ {
					it.Next()
				}
				it.Close()
			}
		}(r)
	}
	for i := 500; i < 4000; i++ {
		put(t, tr, key(i), "v", uint64(i+1), all)
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// Random ops checked against a map, then checked again after reopening.
func TestRandomAgainstMap(t *testing.T) {
	for seed := int64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			tr, p, path := openTree(t, 16)
			model := map[string]string{}
			for seq := uint64(1); seq <= 4000; seq++ {
				k := fmt.Sprintf("k%04d", rng.Intn(800))
				switch r := rng.Intn(10); {
				case r < 6:
					n := rng.Intn(300)
					if rng.Intn(50) == 0 {
						n = 5000 + rng.Intn(20000)
					}
					v := fmt.Sprint(seq, ":") + string(bytes.Repeat([]byte("v"), n))
					put(t, tr, k, v, seq, all)
					model[k] = v
				case r < 8:
					del(t, tr, k, seq, all)
					delete(model, k)
				default:
					want, ok := model[k]
					expect(t, tr, k, all, want, ok)
				}
				if seq%1000 == 0 {
					if err := tr.Prune(all, true); err != nil {
						t.Fatal(err)
					}
				}
			}
			check := func(tr *btree.Tree) {
				t.Helper()
				keys, vals := scan(t, tr, nil, all)
				if len(keys) != len(model) || tr.LiveCount() != len(model) {
					t.Fatalf("scan=%d LiveCount=%d model=%d", len(keys), tr.LiveCount(), len(model))
				}
				for i, k := range keys {
					if model[k] != vals[i] {
						t.Fatalf("key %s: value mismatch", k)
					}
				}
			}
			check(tr)

			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			tr2, err := btree.Open(openPool(t, path, 16))
			if err != nil {
				t.Fatal(err)
			}
			check(tr2)
		})
	}
}
