// Package engine is the single-writer core of DoraDB with same-process MVCC reads.
//
// Write path (Commit):
//  1. queue request to the writer goroutine
//  2. append WAL frames for each op + OpCommit, one Sync
//  3. apply same records to heap pages + update RAM index
//  4. publish committedSequence, reply OK
//  5. maybe checkpoint (flush dirty pages, truncate WAL)
//
// Read path (Get/Has): pin committedSequence, GetVisible, read page slot — does not wait on writer.
//
// Recovery on Open: scan heap pages → replay committed WAL txs (OpCommit) → checkpoint.
package engine

import (
	"errors"
	"fmt"
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
	CachePages        int
	DirtyFrames       int
	LastCheckpoint    time.Time
}

type opKind int

const (
	opCommit opKind = iota
	opStats
	opClose
)

// Mutation is one Set or Delete in a Commit.
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

	pinMu          sync.Mutex
	pinCond        *sync.Cond
	pins           map[uint64]int // snapshotSequence → refcount
	pinCount       int
	vacuuming      bool
	vacuumPending  bool

	fatal  atomic.Pointer[error]
	reqCh  chan request
	closed atomic.Bool
	done   chan struct{}
}

var (
	ErrNotFound = errors.New("engine: not found")
	ErrClosed   = errors.New("engine: closed")
	// ErrFatal means an unrecoverable I/O error; Close and reopen to recover from WAL.
	ErrFatal = errors.New("engine: fatal I/O error; close and reopen")
)

// Open locks the DB, opens main+WAL via the OS VFS, recovers, then starts the writer loop.
func Open(path string, opts Options) (*Engine, error) {
	return OpenWithFS(path, vfs.OS(), opts)
}

// OpenWithFS is like Open but uses fs for all durable I/O (tests / fault injection).
func OpenWithFS(path string, fs vfs.FS, opts Options) (*Engine, error) {
	if path == "" {
		return nil, errors.New("engine: empty path")
	}
	if fs == nil {
		fs = vfs.OS()
	}
	opts = opts.Normalize()
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

	main, err := storage.OpenStore(fs, path, true, storage.OpenOptions{
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

func (e *Engine) poison(err error) {
	if err == nil {
		return
	}
	wrapped := fmt.Errorf("%w: %v", ErrFatal, err)
	e.fatal.CompareAndSwap(nil, &wrapped)
}

func (e *Engine) fatalErr() error {
	if p := e.fatal.Load(); p != nil {
		return *p
	}
	return nil
}

// recover: load index from heap pages, then replay committed WAL txs.
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

	v2SelfCommit := e.log.Version() < 3
	var pending []encode.Record

	applyRec := func(rec encode.Record) error {
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
	}

	err := e.log.Replay(from, func(fv encode.FrameView) error {
		rec := fv.Record
		if rec.Op == encode.OpCommit {
			for _, r := range pending {
				if err := applyRec(r); err != nil {
					return err
				}
			}
			pending = pending[:0]
			if rec.Sequence > e.sequence {
				e.sequence = rec.Sequence
			}
			return nil
		}
		if v2SelfCommit {
			return applyRec(rec)
		}
		pending = append(pending, rec)
		return nil
	})
	if err != nil {
		return err
	}
	// Trailing uncommitted records (no OpCommit) are dropped.

	// Always checkpoint after recovery so the WAL is truncated past any torn
	// tail and main reflects the recovered sequence (fixes double-crash data loss).
	if err := e.checkpointNoVacuum(); err != nil {
		return err
	}
	e.committedSequence.Store(e.sequence)
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
// Blocks while vacuum is in progress so readers never see half-moved slots.
func (e *Engine) Pin(sequence uint64) bool {
	e.pinMu.Lock()
	defer e.pinMu.Unlock()
	for {
		if e.closed.Load() || e.fatal.Load() != nil {
			return false
		}
		if !e.vacuuming {
			break
		}
		e.pinCond.Wait()
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

// Err returns ErrFatal or ErrClosed if the engine is unusable, else nil.
func (e *Engine) Err() error {
	if err := e.fatalErr(); err != nil {
		return err
	}
	if e.closed.Load() {
		return ErrClosed
	}
	return nil
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

// tryBeginVacuum returns true if vacuum may proceed (no pins). Otherwise marks
// vacuumPending and returns false without blocking the writer.
func (e *Engine) tryBeginVacuum() bool {
	e.pinMu.Lock()
	defer e.pinMu.Unlock()
	if e.pinCount > 0 {
		e.vacuumPending = true
		return false
	}
	e.vacuuming = true
	e.vacuumPending = false
	return true
}

func (e *Engine) endVacuum() {
	e.pinMu.Lock()
	e.vacuuming = false
	e.pinCond.Broadcast()
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
		case opCommit:
			e.handleCommit(req)
		}
	}
}

func (e *Engine) handleStats(req request) {
	req.reply <- reply{stats: e.snapshotStats()}
}

func (e *Engine) snapshotStats() Stats {
	live := e.idx.ApproxLiveBytes()
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
		LiveKeys:          e.idx.LiveCount(),
		MainBytes:         mainSize,
		WALBytes:          e.log.Size(),
		ApproxDeadBytes:   dead,
		CachePages:        e.main.CachePages(),
		DirtyFrames:       e.main.DirtyFrames(),
		LastCheckpoint:    e.lastCheckpoint,
	}
}

func (e *Engine) approxDeadBytes() int64 {
	live := e.idx.ApproxLiveBytes()
	dead := e.main.Size() - int64(encode.HeaderSize) - live
	if dead < 0 {
		return 0
	}
	return dead
}

func (e *Engine) handleCommit(req request) {
	if err := e.fatalErr(); err != nil {
		req.reply <- reply{err: err}
		return
	}
	err := e.applyCommit(req.batch)
	req.reply <- reply{err: err}
	if err == nil {
		e.maybeCheckpoint()
	}
}

// applyCommit: WAL append ops + OpCommit → Sync → heap + index → publish.
func (e *Engine) applyCommit(ops []Mutation) error {
	if len(ops) == 0 {
		return nil
	}
	type prepared struct {
		rec      encode.Record
		frameLen int64
	}
	items := make([]prepared, 0, len(ops))
	var totalFrameLen int64
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
			e.poison(err)
			return e.fatalErr()
		}
		items = append(items, prepared{rec: rec, frameLen: int64(n)})
		totalFrameLen += int64(n)
	}
	commitSeq := e.sequence
	_, n, err := e.log.Append(encode.Record{Sequence: commitSeq, Op: encode.OpCommit})
	if err != nil {
		e.poison(err)
		return e.fatalErr()
	}
	totalFrameLen += int64(n)

	if e.opts.SyncWAL {
		if err := e.log.Sync(); err != nil {
			e.poison(err)
			return e.fatalErr()
		}
	}

	var appliedHigh uint64
	for _, it := range items {
		loc, err := e.main.Append(it.rec)
		if err != nil {
			e.poison(err)
			return e.fatalErr()
		}
		e.idx.Apply(it.rec.Key, loc)
		appliedHigh = it.rec.Sequence
	}
	e.walBytesSinceCheckpoint += totalFrameLen
	e.mutationsSinceCheckpoint = true
	e.committedSequence.Store(appliedHigh)
	return nil
}

// Commit durably applies ops as one atomic publish (WAL OpCommit marker).
func (e *Engine) Commit(ops []Mutation) error {
	if len(ops) == 0 {
		return nil
	}
	if err := e.fatalErr(); err != nil {
		return err
	}
	batch := make([]Mutation, len(ops))
	for i, op := range ops {
		batch[i] = Mutation{
			Delete: op.Delete,
			Key:    append([]byte(nil), op.Key...),
			Value:  append([]byte(nil), op.Value...),
		}
	}
	return e.call(request{kind: opCommit, batch: batch}).err
}

// ApplyBatch is an alias for Commit (kept for transitional call sites).
func (e *Engine) ApplyBatch(ops []Mutation) error {
	return e.Commit(ops)
}

func (e *Engine) maybeCheckpoint() {
	if e.checkpointInFlight {
		return
	}
	need := e.mutationsSinceCheckpoint &&
		(e.walBytesSinceCheckpoint >= e.opts.WALCheckpointBytes ||
			time.Since(e.lastCheckpoint) >= e.opts.WALCheckpointInterval)

	e.pinMu.Lock()
	pending := e.vacuumPending
	e.pinMu.Unlock()

	if !need && !pending {
		return
	}
	if err := e.checkpoint(); err != nil {
		e.poison(err)
	}
}

// checkpointNoVacuum flushes dirty pages, updates meta, truncates WAL (no vacuum).
func (e *Engine) checkpointNoVacuum() error {
	h := e.main.Header()
	h.Sequence = e.sequence
	h.WalCheckpointOffset = encode.HeaderSize
	h.FormatVersion = encode.FormatVersion
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

// checkpoint flushes dirty pages, advances meta sequence / WAL offset, truncates WAL.
// Page Locs stay stable, so reader pins do not block flush.
func (e *Engine) checkpoint() error {
	e.checkpointInFlight = true
	defer func() { e.checkpointInFlight = false }()

	if e.mutationsSinceCheckpoint {
		if err := e.checkpointNoVacuum(); err != nil {
			return err
		}
	}
	return e.maybeVacuum()
}

// maybeVacuum runs heap vacuum when dead bytes exceed the threshold, or when
// a previous attempt was deferred (vacuumPending). Skips (does not block) when
// readers are pinned.
func (e *Engine) maybeVacuum() error {
	if e.opts.VacuumDeadBytes <= 0 {
		return nil
	}
	e.pinMu.Lock()
	pending := e.vacuumPending
	e.pinMu.Unlock()
	if !pending && e.approxDeadBytes() < e.opts.VacuumDeadBytes {
		return nil
	}
	return e.vacuum()
}

func (e *Engine) vacuum() error {
	if !e.tryBeginVacuum() {
		return nil
	}
	defer e.endVacuum()

	horizon := e.committedSequence.Load()
	e.idx.Prune(horizon)
	if err := e.main.Vacuum(e.idx); err != nil {
		return err
	}
	h := e.main.Header()
	h.Sequence = e.sequence
	if err := e.main.SetHeader(h); err != nil {
		return err
	}
	return e.main.FlushAll()
}

func (e *Engine) finishClose(req request) {
	e.closed.Store(true)
	e.waitPins()

	var err error
	if e.fatalErr() == nil {
		if syncErr := e.log.Sync(); err == nil {
			err = syncErr
		}
		h := e.main.Header()
		h.Sequence = e.sequence
		if setErr := e.main.SetHeader(h); err == nil {
			err = setErr
		}
		if flushErr := e.main.FlushAll(); err == nil {
			err = flushErr
		}
	}
	err1 := e.log.Close()
	err2 := e.main.Close()
	var err3 error
	if e.lk != nil {
		err3 = e.lk.Unlock()
		e.lk = nil
	}
	if err == nil {
		err = err1
	}
	if err == nil {
		err = err2
	}
	if err == nil {
		err = err3
	}
	req.reply <- reply{err: err}
}

func (e *Engine) call(req request) reply {
	// Close must always reach the writer loop so files unlock; other ops fail fast when poisoned.
	if req.kind != opClose {
		if err := e.fatalErr(); err != nil {
			return reply{err: err}
		}
		if e.closed.Load() {
			return reply{err: ErrClosed}
		}
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
	if err := e.fatalErr(); err != nil {
		return nil, err
	}
	if e.closed.Load() {
		return nil, ErrClosed
	}
	if !e.Pin(snapshotSequence) {
		if err := e.fatalErr(); err != nil {
			return nil, err
		}
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
	if err := e.fatalErr(); err != nil {
		return false, err
	}
	if e.closed.Load() {
		return false, ErrClosed
	}
	if !e.Pin(snapshotSequence) {
		if err := e.fatalErr(); err != nil {
			return false, err
		}
		return false, ErrClosed
	}
	defer e.Unpin(snapshotSequence)

	_, ok := e.idx.GetVisible(key, snapshotSequence)
	return ok, nil
}

func (e *Engine) Stats() Stats {
	return e.call(request{kind: opStats}).stats
}

func (e *Engine) Close() error {
	r := e.call(request{kind: opClose})
	<-e.done
	return r.err
}
