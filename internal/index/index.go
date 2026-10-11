// Package index is the in-RAM map from key → where the value lives on disk.
// Values themselves stay in the main file; we only store offsets here.
package index

// Loc points at one record in the main *.dorradb file.
type Loc struct {
	Offset   int64 // start of the framed record
	Length   int64 // full frame length
	ValueOff int64 // where the value bytes start (for ReadAt)
	ValueLen int
	Seq      uint64 // version of this entry
	Deleted  bool   // tombstone until the next compact
}

// Index is key → latest Loc. Not safe for concurrent use without external sync
// (the engine writer loop owns it).
type Index struct {
	m map[string]Loc
}

func New() *Index {
	return &Index{m: make(map[string]Loc)}
}

func (idx *Index) Get(key []byte) (Loc, bool) {
	loc, ok := idx.m[string(key)]
	return loc, ok
}

func (idx *Index) Put(key []byte, loc Loc) {
	idx.m[string(key)] = loc
}

func (idx *Index) Delete(key []byte, seq uint64) {
	idx.m[string(key)] = Loc{Seq: seq, Deleted: true}
}

// Apply keeps the Loc with the highest seq (needed when replaying WAL after
// some records may already be on the main file).
func (idx *Index) Apply(key []byte, loc Loc) {
	prev, ok := idx.m[string(key)]
	if ok && prev.Seq > loc.Seq {
		return
	}
	idx.m[string(key)] = loc
}

func (idx *Index) LiveCount() int {
	n := 0
	for _, loc := range idx.m {
		if !loc.Deleted {
			n++
		}
	}
	return n
}

func (idx *Index) ApproxLiveBytes() int64 {
	var n int64
	for _, loc := range idx.m {
		if !loc.Deleted {
			n += loc.Length
		}
	}
	return n
}

// ForEachLive visits non-deleted keys (used when building a checkpoint).
func (idx *Index) ForEachLive(fn func(key string, loc Loc) error) error {
	for k, loc := range idx.m {
		if loc.Deleted {
			continue
		}
		if err := fn(k, loc); err != nil {
			return err
		}
	}
	return nil
}

func (idx *Index) Reset() {
	idx.m = make(map[string]Loc)
}

func (idx *Index) MaxSeq() uint64 {
	var max uint64
	for _, loc := range idx.m {
		if loc.Seq > max {
			max = loc.Seq
		}
	}
	return max
}
