// Package engine is the single-writer core of DoraDB with same-process MVCC reads.
//
// Write path (Set/Delete):
//  1. queue request to the writer goroutine
//  2. group-commit: append several WAL records, one Sync
//  3. apply same records to heap pages + update RAM index
//  4. publish committedSequence, reply OK
//  5. maybe checkpoint (flush dirty pages, truncate WAL)
//
// Read path (Get/Has): pin committedSequence, GetVisible, read page slot — does not wait on writer.
//
// Recovery on Open: scan heap pages → replay WAL records with sequence > main max sequence.
package engine

import (
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"dorradb/internal/encode"
	"dorradb/internal/index"
	"dorradb/internal/storage"
	"dorradb/internal/vfs"
	"dorradb/internal/wal"
)

type Stats struct {
	Path              string
	Sequence          uint64
	CommittedSequence uint64
	PinnedReaders     int
	LiveKeys          int
	MainBytes         int64
	WALBytes          int64
	ApproxDeadBytes   int64
	LastCheckpoint    time.Time
}

type opKind int

const (
	opSet opKind = iota
	opDelete
	opBatch
	opStats
	opClose
)

// Mutation is one Set or Delete in an ApplyBatch.
type Mutation struct {
	Delete bool
	Key    []byte
	Value  []byte // ignored when Delete is true
}

type request struct {
	kind  opKind
	key   []byte
	value []byte
	batch []Mutation
	reply chan reply
}

type reply struct {
	stats Stats
	err   error
}

// Engine owns the lock, main store, WAL, index, and the writer loop.
type Engine struct {
	path  string
	fs    vfs.FS
	lk    vfs.Lock
	main  *storage.Store
	log   *wal.Log
	idx   *index.Index
	opts  Options

	sequence                 uint64 // next mutation gets sequence+1 (writer-only)
	committedSequence        atomic.Uint64
	walBytesSinceCheckpoint  int64
	lastCheckpoint           time.Time
	mutationsSinceCheckpoint bool
	checkpointInFlight       bool

	pinMu    sync.Mutex
	pinCond  *sync.Cond
	pins     map[uint64]int // snapshotSequence → refcount
	pinCount int

	reqCh  chan request
	closed atomic.Bool
	done   chan struct{}
}

var (
	ErrNotFound = errors.New("engine: not found")
	ErrClosed   = errors.New("engine: closed")
)

// Open locks the DB, opens main+WAL via VFS, recovers, then starts the writer loop.
func Open(path string, opts Options) (*Engine, error) {
	if path == "" {
		return nil, errors.New("engine: empty path")
	}
	opts = opts.Normalize()
	fs := vfs.OS()
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := fs.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	lk, err := fs.Lock(path + ".lock")
	if err != nil {
		return nil, err
	}

	main, err := storage.OpenStore(fs, path, true)
	if err != nil {
		_ = lk.Unlock()
		return nil, err
	}
	log, err := wal.Open(fs, path, true)
	if err != nil {
		_ = main.Close()
		_ = lk.Unlock()
		return nil, err
	}

	e := &Engine{
		path:           path,
		fs:             fs,
		lk:             lk,
		main:           main,
		log:            log,
		idx:            index.New(),
		opts:           opts,
		lastCheckpoint: time.Now(),
		pins:           make(map[uint64]int),
		reqCh:          make(chan request, 256),
		done:           make(chan struct{}),
	}
	e.pinCond = sync.NewCond(&e.pinMu)

	if err := e.recover(); err != nil {
		e.cleanup()
		return nil, err
	}

	go e.loop()
	return e, nil
}

// recover: load index from heap pages, then replay WAL for sequences not yet on main.
func (e *Engine) recover() error {
	if err := e.main.ScanIndex(e.idx); err != nil {
		return err
	}
	maxMainSequence := e.idx.MaxSequence()
	if hseq := e.main.Header().Sequence; hseq > maxMainSequence {
		maxMainSequence = hseq
	}
	e.sequence = maxMainSequence

	from := int64(e.main.Header().WalCheckpointOffset)
	if from < encode.HeaderSize {
		from = encode.HeaderSize
	}
	err := e.log.Replay(from, func(fv encode.FrameView) error {
		rec := fv.Record
		if rec.Sequence > e.sequence {
			e.sequence = rec.Sequence
		}
		if rec.Sequence <= maxMainSequence {
			return nil
		}
		loc, aerr := e.main.Append(rec)
		if aerr != nil {
			return aerr
		}
		e.idx.Apply(rec.Key, loc)
		return nil
	})
	if err != nil {
		return err
	}
	// Persist pages applied from WAL so a second crash does not re-apply incorrectly
	// relative to header.Sequence — bump sequence on meta after recovery apply.
	if e.sequence > e.main.Header().Sequence {
		h := e.main.Header()
		h.Sequence = e.sequence
		if err := e.main.SetHeader(h); err != nil {
			return err
		}
		if err := e.main.FlushAll(); err != nil {
			return err
		}
	}
	e.committedSequence.Store(e.sequence)
	e.walBytesSinceCheckpoint = e.log.Size() - encode.HeaderSize
	if e.walBytesSinceCheckpoint < 0 {
		e.walBytesSinceCheckpoint = 0
	}
	return nil
}

func (e *Engine) cleanup() {
	if e.log != nil {
		e.log.Close()
	}
	if e.main != nil {
		e.main.Close()
	}
	if e.lk != nil {
		_ = e.lk.Unlock()
	}
}

// Pin holds a snapshot so Prune will not drop versions the reader still needs.
func (e *Engine) Pin(sequence uint64) bool {
	e.pinMu.Lock()
	defer e.pinMu.Unlock()
	if e.closed.Load() {
		return false
	}
	e.pins[sequence]++
	e.pinCount++
	return true
}

// Unpin releases a Pin. Extra Unpin calls are a no-op (no underflow).
func (e *Engine) Unpin(sequence uint64) {
	e.pinMu.Lock()
	defer e.pinMu.Unlock()
	n := e.pins[sequence]
	if n <= 0 {
		return
	}
	if n == 1 {
		delete(e.pins, sequence)
	} else {
		e.pins[sequence] = n - 1
	}
	e.pinCount--
	if e.pinCount == 0 {
		e.pinCond.Broadcast()
	}
}

// CommittedSequence is the last sequence fully published to readers.
func (e *Engine) CommittedSequence() uint64 {
	return e.committedSequence.Load()
}

func (e *Engine) pinnedReaderCount() int {
	e.pinMu.Lock()
	defer e.pinMu.Unlock()
	return e.pinCount
}

func (e *Engine) waitPins() {
	e.pinMu.Lock()
	for e.pinCount > 0 {
		e.pinCond.Wait()
	}
	e.pinMu.Unlock()
}

func (e *Engine) loop() {
	defer close(e.done)
	for {
		req, ok := <-e.reqCh
		if !ok {
			return
		}
		switch req.kind {
		case opClose:
			e.finishClose(req)
			return
		case opStats:
			e.handleStats(req)
		case opBatch:
			e.handleBatch(req)
		case opSet, opDelete:
			e.handleMutations(req)
		}
	}
}

func (e *Engine) handleStats(req request) {
	live := e.idx.ApproxLiveBytes()
	mainSize := e.main.Size()
	dead := mainSize - int64(encode.HeaderSize) - live
	if dead < 0 {
		dead = 0
	}
	req.reply <- reply{stats: Stats{
		Path:              e.path,
		Sequence:          e.sequence,
		CommittedSequence: e.committedSequence.Load(),
		PinnedReaders:     e.pinnedReaderCount(),
		LiveKeys:          e.idx.LiveCount(),
		MainBytes:         mainSize,
		WALBytes:          e.log.Size(),
		ApproxDeadBytes:   dead,
		LastCheckpoint:    e.lastCheckpoint,
	}}
}

func (e *Engine) handleMutations(first request) {
	group := []request{first}
	deadline := time.Now().Add(e.opts.GroupCommitMaxWait)
gather:
	for len(group) < e.opts.GroupCommitMaxOps {
		wait := time.Until(deadline)
		if wait <= 0 {
			break
		}
		timer := time.NewTimer(wait)
		select {
		case req := <-e.reqCh:
			timer.Stop()
			switch req.kind {
			case opSet, opDelete:
				group = append(group, req)
			case opBatch:
				e.applyGroup(group)
				e.handleBatch(req)
				return
			case opClose:
				e.applyGroup(group)
				e.finishClose(req)
				return
			case opStats:
				e.handleStats(req)
			}
		case <-timer.C:
			break gather
		}
	}
	e.applyGroup(group)
	e.maybeCheckpoint()
}

func (e *Engine) handleBatch(req request) {
	err := e.applyMutations(req.batch)
	req.reply <- reply{err: err}
	if err == nil {
		e.maybeCheckpoint()
	}
}

func (e *Engine) applyGroup(group []request) {
	if len(group) == 0 {
		return
	}
	ops := make([]Mutation, len(group))
	for i, req := range group {
		ops[i] = Mutation{
			Delete: req.kind == opDelete,
			Key:    req.key,
			Value:  req.value,
		}
	}
	err := e.applyMutations(ops)
	for _, r := range group {
		r.reply <- reply{err: err}
	}
}

// applyMutations: WAL append all → Sync once → heap pages + index → publish committedSequence.
func (e *Engine) applyMutations(ops []Mutation) error {
	if len(ops) == 0 {
		return nil
	}
	type prepared struct {
		rec      encode.Record
		frameLen int64
	}
	items := make([]prepared, 0, len(ops))
	for _, op := range ops {
		e.sequence++
		rec := encode.Record{Sequence: e.sequence, Key: op.Key}
		if op.Delete {
			rec.Op = encode.OpDelete
		} else {
			rec.Op = encode.OpSet
			rec.Value = op.Value
		}
		_, n, err := e.log.Append(rec)
		if err != nil {
			return err
		}
		items = append(items, prepared{rec: rec, frameLen: int64(n)})
	}
	if e.opts.SyncWAL {
		if err := e.log.Sync(); err != nil {
			return err
		}
	}

	var appliedHigh uint64
	for _, it := range items {
		loc, err := e.main.Append(it.rec)
		if err != nil {
			return err
		}
		e.idx.Apply(it.rec.Key, loc)
		appliedHigh = it.rec.Sequence
		e.walBytesSinceCheckpoint += it.frameLen
		e.mutationsSinceCheckpoint = true
	}
	e.committedSequence.Store(appliedHigh)
	return nil
}

// ApplyBatch durably applies ops as one atomic publish.
func (e *Engine) ApplyBatch(ops []Mutation) error {
	if len(ops) == 0 {
		return nil
	}
	batch := make([]Mutation, len(ops))
	for i, op := range ops {
		batch[i] = Mutation{
			Delete: op.Delete,
			Key:    append([]byte(nil), op.Key...),
			Value:  append([]byte(nil), op.Value...),
		}
	}
	return e.call(request{kind: opBatch, batch: batch}).err
}

func (e *Engine) maybeCheckpoint() {
	if e.checkpointInFlight || !e.mutationsSinceCheckpoint {
		return
	}
	bySize := e.walBytesSinceCheckpoint >= e.opts.WALCheckpointBytes
	byTime := time.Since(e.lastCheckpoint) >= e.opts.WALCheckpointInterval
	if bySize || byTime {
		_ = e.checkpoint()
	}
}

// checkpoint flushes dirty pages, advances meta sequence / WAL offset, truncates WAL.
// Page Locs stay stable, so reader pins do not block flush.
func (e *Engine) checkpoint() error {
	e.checkpointInFlight = true
	defer func() { e.checkpointInFlight = false }()

	h := e.main.Header()
	h.Sequence = e.sequence
	h.WalCheckpointOffset = encode.HeaderSize
	if err := e.main.SetHeader(h); err != nil {
		return err
	}
	if err := e.main.FlushAll(); err != nil {
		return err
	}
	if err := e.log.TruncateReset(); err != nil {
		return err
	}
	e.walBytesSinceCheckpoint = 0
	e.mutationsSinceCheckpoint = false
	e.lastCheckpoint = time.Now()
	return nil
}

func (e *Engine) finishClose(req request) {
	e.closed.Store(true)
	e.waitPins()
	_ = e.log.Sync()
	// Flush pages so main reflects committed work.
	h := e.main.Header()
	h.Sequence = e.sequence
	_ = e.main.SetHeader(h)
	_ = e.main.FlushAll()
	err1 := e.log.Close()
	err2 := e.main.Close()
	var err3 error
	if e.lk != nil {
		err3 = e.lk.Unlock()
		e.lk = nil
	}
	err := err1
	if err == nil {
		err = err2
	}
	if err == nil {
		err = err3
	}
	req.reply <- reply{err: err}
}

func (e *Engine) call(req request) reply {
	if e.closed.Load() {
		return reply{err: ErrClosed}
	}
	req.reply = make(chan reply, 1)
	select {
	case e.reqCh <- req:
		return <-req.reply
	case <-e.done:
		return reply{err: ErrClosed}
	}
}

func (e *Engine) Get(key []byte) ([]byte, error) {
	return e.GetAt(key, e.committedSequence.Load())
}

func (e *Engine) GetAt(key []byte, snapshotSequence uint64) ([]byte, error) {
	if e.closed.Load() {
		return nil, ErrClosed
	}
	if !e.Pin(snapshotSequence) {
		return nil, ErrClosed
	}
	defer e.Unpin(snapshotSequence)

	loc, ok := e.idx.GetVisible(key, snapshotSequence)
	if !ok {
		return nil, ErrNotFound
	}
	val, err := e.main.ReadValue(loc)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), val...), nil
}

func (e *Engine) Has(key []byte) (bool, error) {
	return e.HasAt(key, e.committedSequence.Load())
}

func (e *Engine) HasAt(key []byte, snapshotSequence uint64) (bool, error) {
	if e.closed.Load() {
		return false, ErrClosed
	}
	if !e.Pin(snapshotSequence) {
		return false, ErrClosed
	}
	defer e.Unpin(snapshotSequence)

	_, ok := e.idx.GetVisible(key, snapshotSequence)
	return ok, nil
}

func (e *Engine) Set(key, value []byte) error {
	return e.call(request{
		kind:  opSet,
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
	}).err
}

func (e *Engine) Delete(key []byte) error {
	return e.call(request{kind: opDelete, key: append([]byte(nil), key...)}).err
}

func (e *Engine) Stats() Stats {
	return e.call(request{kind: opStats}).stats
}

func (e *Engine) Close() error {
	r := e.call(request{kind: opClose})
	<-e.done
	return r.err
}
