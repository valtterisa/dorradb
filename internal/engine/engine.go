// Package engine is the single-writer core of DoraDB with same-process MVCC reads.
//
// Write path (Set/Delete):
//  1. queue request to the writer goroutine
//  2. group-commit: append several WAL records, one Sync
//  3. apply same records to the clustered B+ tree
//  4. publish committedSequence, reply OK
//  5. maybe checkpoint (flush dirty pages, truncate WAL)
//
// Read path (Get/Has): pin committedSequence, tree.GetVisible — does not wait on writer.
//
// Recovery on Open: load table root → replay WAL records with sequence > main header sequence.
package engine

import (
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"dorradb/internal/btree"
	"dorradb/internal/encode"
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
	CachePages        int
	DirtyFrames       int
	LastCheckpoint    time.Time
}

type opKind int

const (
	opBatch opKind = iota
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
	batch []Mutation
	reply chan reply
}

type reply struct {
	stats Stats
	err   error
}

// Engine owns the lock, main store, WAL, table tree, and the writer loop.
type Engine struct {
	path  string
	fs    vfs.FS
	lk    vfs.Lock
	main  *storage.BufferPool
	log   *wal.Log
	tree  *btree.Tree
	opts  Options

	sequence                 uint64 // last sequence handed out (writer-only)
	appliedSequence          uint64 // last sequence applied to the tree (writer-only)
	committedSequence        atomic.Uint64
	walBytesSinceCheckpoint  int64
	lastCheckpoint           time.Time
	mutationsSinceCheckpoint bool
	checkpointInFlight       bool

	pinMu    sync.Mutex
	pinCond  *sync.Cond
	pins     map[uint64]int
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

	main, err := storage.Open(fs, path, true, storage.OpenOptions{
		CachePages: opts.CachePages,
	})
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

	tree, err := btree.Open(main)
	if err != nil {
		_ = log.Close()
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
		tree:           tree,
		opts:           opts,
		lastCheckpoint: time.Now(),
		pins:           make(map[uint64]int),
		reqCh:          make(chan request, 256),
		done:           make(chan struct{}),
	}
	e.pinCond = sync.NewCond(&e.pinMu)
	tree.SetFlusher(e.flushPages)

	if err := e.recover(); err != nil {
		e.cleanup()
		return nil, err
	}

	go e.loop()
	return e, nil
}

// recover replays WAL records newer than the last flush. The main file holds
// exactly the tree as of header.Sequence (pages only reach disk via whole,
// journaled flushes), so everything after it comes from the WAL.
func (e *Engine) recover() error {
	flushed := e.main.Header().Sequence
	e.sequence = flushed
	e.appliedSequence = flushed

	from := int64(e.main.Header().WalCheckpointOffset)
	if from < encode.HeaderSize {
		from = encode.HeaderSize
	}
	const noReaders = ^uint64(0)
	err := e.log.Replay(from, func(fv encode.FrameView) error {
		rec := fv.Record
		if rec.Sequence > e.sequence {
			e.sequence = rec.Sequence
		}
		if rec.Sequence <= flushed {
			return nil
		}
		var err error
		if rec.Op == encode.OpDelete {
			err = e.tree.Delete(rec.Key, rec.Sequence, noReaders)
		} else {
			err = e.tree.Put(rec.Key, rec.Value, rec.Sequence, noReaders)
		}
		if err == nil {
			e.appliedSequence = rec.Sequence
		}
		return err
	})
	if err != nil {
		return err
	}
	if e.appliedSequence > flushed {
		if err := e.flushPages(); err != nil {
			return err
		}
	}
	e.committedSequence.Store(e.appliedSequence)
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

// flushPages writes all dirty pages with the header describing exactly what
// they contain (applied sequence + root). The WAL is left alone.
func (e *Engine) flushPages() error {
	h := e.main.Header()
	h.Sequence = e.appliedSequence
	h.TableRoot = e.tree.Root()
	if err := e.main.SetHeader(h); err != nil {
		return err
	}
	return e.main.FlushAll()
}

// PinLatest pins the newest committed snapshot and returns it. Loading and
// pinning happen under one lock so pruning can never race between them.
func (e *Engine) PinLatest() (uint64, bool) {
	e.pinMu.Lock()
	defer e.pinMu.Unlock()
	if e.closed.Load() {
		return 0, false
	}
	seq := e.committedSequence.Load()
	e.pins[seq]++
	e.pinCount++
	return seq, true
}

// Pin holds a snapshot the caller already has pinned, so pruning keeps its versions.
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

// pruneHorizon is the oldest snapshot any reader may still use.
func (e *Engine) pruneHorizon() uint64 {
	e.pinMu.Lock()
	defer e.pinMu.Unlock()
	h := e.committedSequence.Load()
	for seq := range e.pins {
		if seq < h {
			h = seq
		}
	}
	return h
}

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

func (e *Engine) waitPins() {
	e.pinMu.Lock()
	for e.pinCount > 0 {
		e.pinCond.Wait()
	}
	e.pinMu.Unlock()
}

func (e *Engine) pinnedReaderCount() int {
	e.pinMu.Lock()
	defer e.pinMu.Unlock()
	return e.pinCount
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
		}
	}
}

func (e *Engine) handleStats(req request) {
	req.reply <- reply{stats: e.snapshotStats()}
}

func (e *Engine) snapshotStats() Stats {
	live := e.tree.ApproxLiveBytes()
	mainSize := e.main.Size()
	dead := mainSize - int64(encode.HeaderSize) - live
	if dead < 0 {
		dead = 0
	}
	return Stats{
		Path:              e.path,
		Sequence:          e.sequence,
		CommittedSequence: e.committedSequence.Load(),
		PinnedReaders:     e.pinnedReaderCount(),
		LiveKeys:          e.tree.LiveCount(),
		MainBytes:         mainSize,
		WALBytes:          e.log.Size(),
		ApproxDeadBytes:   dead,
		CachePages:        e.main.CachePages(),
		DirtyFrames:       e.main.DirtyFrames(),
		LastCheckpoint:    e.lastCheckpoint,
	}
}

func (e *Engine) approxDeadBytes() int64 {
	live := e.tree.ApproxLiveBytes()
	dead := e.main.Size() - int64(encode.HeaderSize) - live
	if dead < 0 {
		return 0
	}
	return dead
}

func (e *Engine) handleBatch(req request) {
	err := e.applyMutations(req.batch)
	req.reply <- reply{err: err}
	if err == nil {
		e.maybeCheckpoint()
	}
}

// applyMutations: WAL append all → Sync once → tree Put/Delete → publish committedSequence.
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

	// New readers pin at committedSequence or later, so this horizon stays
	// valid for the whole batch.
	horizon := e.pruneHorizon()
	for _, it := range items {
		var err error
		if it.rec.Op == encode.OpDelete {
			err = e.tree.Delete(it.rec.Key, it.rec.Sequence, horizon)
		} else {
			err = e.tree.Put(it.rec.Key, it.rec.Value, it.rec.Sequence, horizon)
		}
		if err != nil {
			return err
		}
		e.appliedSequence = it.rec.Sequence
		e.walBytesSinceCheckpoint += it.frameLen
		e.mutationsSinceCheckpoint = true
	}
	e.committedSequence.Store(e.appliedSequence)
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

func (e *Engine) checkpoint() error {
	e.checkpointInFlight = true
	defer func() { e.checkpointInFlight = false }()

	if err := e.flushPages(); err != nil {
		return err
	}
	if err := e.log.TruncateReset(); err != nil {
		return err
	}
	e.walBytesSinceCheckpoint = 0
	e.mutationsSinceCheckpoint = false
	e.lastCheckpoint = time.Now()
	return e.maybeVacuum()
}

func (e *Engine) maybeVacuum() error {
	if e.opts.VacuumDeadBytes <= 0 {
		return nil
	}
	if e.approxDeadBytes() < e.opts.VacuumDeadBytes {
		return nil
	}
	return e.vacuum()
}

func (e *Engine) vacuum() error {
	e.waitPins()
	if err := e.tree.Prune(e.committedSequence.Load(), true); err != nil {
		return err
	}
	return e.flushPages()
}

func (e *Engine) finishClose(req request) {
	e.closed.Store(true)
	e.waitPins()
	_ = e.log.Sync()
	_ = e.flushPages()
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

// GetAt reads at snapshotSequence, which the caller must already hold pinned.
func (e *Engine) GetAt(key []byte, snapshotSequence uint64) ([]byte, error) {
	val, ok, err := e.tree.GetVisible(key, snapshotSequence)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	return val, nil
}

// HasAt checks existence at snapshotSequence, which the caller must already hold pinned.
func (e *Engine) HasAt(key []byte, snapshotSequence uint64) (bool, error) {
	_, ok, err := e.tree.GetVisible(key, snapshotSequence)
	return ok, err
}

func (e *Engine) Stats() Stats {
	return e.call(request{kind: opStats}).stats
}

func (e *Engine) Close() error {
	r := e.call(request{kind: opClose})
	<-e.done
	return r.err
}
