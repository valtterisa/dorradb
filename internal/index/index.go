package index

type Loc struct {
	Offset   int64
	Length   int64
	ValueOff int64
	ValueLen int
	Seq      uint64
	Deleted  bool
}

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
