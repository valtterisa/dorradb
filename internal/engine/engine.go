// Package engine is the single-writer core of DoraDB with same-process MVCC reads.
//
// Write path (Set/Delete):
//  1. queue request to the writer goroutine
//  2. group-commit: append several WAL records, one Sync
//  3. append same records to main file + update RAM index
//  4. publish committedSequence, reply OK
//  5. maybe checkpoint (WAL size or time) if no reader pins
//
// Read path (Get/Has): pin committedSequence, GetVisible, ReadAt — does not wait on writer.
//
// Recovery on Open: scan main → replay WAL records with sequence > main max sequence.
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
	opStats
	opClose
)

type request struct {
	kind  opKind
	key   []byte
	value []byte
	reply chan reply
}

type reply struct {
	stats Stats
	err   error
}

// Engine owns the lock, main file, WAL, index, and the writer loop.
type Engine struct {
	path string
	fs   vfs.FS
	lk   vfs.Lock
	main *storage.Main
	log  *wal.Log
	idx  *index.Index
	opts Options

	sequence           uint64 // next mutation gets sequence+1 (writer-only)
	committedSequence  atomic.Uint64
	walBytesSinceCkpt  int64
	lastCheckpoint     time.Time
	mutationsSinceCkpt bool
	ckptInFlight       bool

	pinMu    sync.Mutex
	pinCond  *sync.Cond
	pins     map[uint64]int // snapshotSequence → refcount
	pinCount int

	reqCh  chan request // mutations / stats / close
	closed atomic.Bool
	done   chan struct{} // closed when writer loop exits
}

var (
	ErrNotFound = errors.New("engine: not found")
	ErrClosed   = errors.New("engine: closed")
)

// Open locks the DB, opens main+WAL via VFS, recovers, then starts the writer loop.
// opts should already be normalized (see DefaultOptions / Normalize).
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

	// Exclusive lock so two processes cannot open the same DB.
	lk, err := fs.Lock(path + ".lock")
	if err != nil {
		return nil, err
	}

	main, err := storage.Open(fs, path, true)
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

// recover: load index from main, then replay WAL for sequences not yet on main.
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
	e.committedSequence.Store(e.sequence)
	e.walBytesSinceCkpt = e.log.Size() - encode.HeaderSize
	if e.walBytesSinceCkpt < 0 {
		e.walBytesSinceCkpt = 0
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

// Pin holds a snapshot so compact will not reclaim Locs the reader still needs.
// Returns false if the engine is closed. Exported for a future Transaction API.
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

// Unpin releases a Pin. Exported for a future Transaction API.
func (e *Engine) Unpin(sequence uint64) {
	e.pinMu.Lock()
	defer e.pinMu.Unlock()
	n := e.pins[sequence]
	if n <= 1 {
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

// loop is the only goroutine that mutates index/main/WAL (aside from index RLock reads).
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
		case opSet, opDelete:
			e.handleMutations(req)
		}
	}
}

func (e *Engine) handleStats(req request) {
	live := e.idx.ApproxLiveBytes()
	mainSize := e.main.Size()
	dead := mainSize - encode.HeaderSize - live
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

// handleMutations gathers a group of Sets/Deletes, then applyGroup (one WAL sync).
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

// applyGroup: WAL append all → Sync once → main + index → publish committedSequence → ack.
// If Sync fails, nothing is applied to main (WAL may still have bytes on disk).
func (e *Engine) applyGroup(group []request) {
	if len(group) == 0 {
		return
	}
	type prepared struct {
		req      request
		rec      encode.Record
		frameLen int64
	}
	items := make([]prepared, 0, len(group))
	for _, req := range group {
		e.sequence++
		rec := encode.Record{Sequence: e.sequence, Key: req.key}
		if req.kind == opSet {
			rec.Op = encode.OpSet
			rec.Value = req.value
		} else {
			rec.Op = encode.OpDelete
		}
		_, n, err := e.log.Append(rec)
		if err != nil {
			for _, r := range group {
				r.reply <- reply{err: err}
			}
			return
		}
		items = append(items, prepared{req: req, rec: rec, frameLen: int64(n)})
	}
	if e.opts.SyncWAL {
		if err := e.log.Sync(); err != nil {
			for _, r := range group {
				r.reply <- reply{err: err}
			}
			return
		}
	}

	type result struct {
		req request
		err error
	}
	results := make([]result, 0, len(items))
	var appliedHigh uint64
	for _, it := range items {
		loc, err := e.main.Append(it.rec)
		if err != nil {
			results = append(results, result{req: it.req, err: err})
			continue
		}
		e.idx.Apply(it.rec.Key, loc)
		appliedHigh = it.rec.Sequence
		e.walBytesSinceCkpt += it.frameLen
		e.mutationsSinceCkpt = true
		results = append(results, result{req: it.req})
	}
	if appliedHigh > 0 {
		e.committedSequence.Store(appliedHigh)
	}
	for _, r := range results {
		r.req.reply <- reply{err: r.err}
	}
}

// maybeCheckpoint triggers compact when WAL grew enough or enough time passed.
func (e *Engine) maybeCheckpoint() {
	if e.ckptInFlight || !e.mutationsSinceCkpt {
		return
	}
	bySize := e.walBytesSinceCkpt >= e.opts.WALCheckpointBytes
	byTime := time.Since(e.lastCheckpoint) >= e.opts.WALCheckpointInterval
	if bySize || byTime {
		_ = e.compact()
	}
}

// compact rewrites only live keys into *.tmp, renames over main, truncates WAL.
// Deferred while any reader pin is held (Loc offsets would become invalid).
func (e *Engine) compact() error {
	if e.pinnedReaderCount() > 0 {
		return nil
	}

	e.ckptInFlight = true
	defer func() { e.ckptInFlight = false }()

	// Re-check under pinMu so a pin cannot slip in after the count check and
	// before we replace the main file. Writers own the loop; readers only pin.
	e.pinMu.Lock()
	if e.pinCount > 0 {
		e.pinMu.Unlock()
		return nil
	}
	e.pinMu.Unlock()

	var records []encode.Record
	err := e.idx.ForEachLive(func(key string, loc index.Loc) error {
		val, err := e.main.ReadValue(loc)
		if err != nil {
			return err
		}
		records = append(records, encode.Record{
			Sequence: loc.Sequence,
			Op:       encode.OpSet,
			Key:      []byte(key),
			Value:    val,
		})
		return nil
	})
	if err != nil {
		return err
	}

	tmp := e.path + ".tmp"
	h := encode.Header{
		FileType:            encode.FileTypeMain,
		FormatVersion:       encode.FormatVersion,
		Sequence:            e.sequence,
		WalCheckpointOffset: encode.HeaderSize,
	}
	newSize, err := storage.WriteCompactFile(e.fs, tmp, h, records)
	if err != nil {
		_ = e.fs.Remove(tmp)
		return err
	}

	// Hold pinMu through replace + rescan so no reader can observe stale Locs.
	e.pinMu.Lock()
	if e.pinCount > 0 {
		e.pinMu.Unlock()
		_ = e.fs.Remove(tmp)
		return nil
	}
	if err := e.main.ReplaceWithCompact(tmp, h, newSize); err != nil {
		e.pinMu.Unlock()
		_ = e.fs.Remove(tmp)
		return err
	}
	if err := e.log.TruncateReset(); err != nil {
		e.pinMu.Unlock()
		return err
	}
	if err := e.main.ScanIndex(e.idx); err != nil {
		e.pinMu.Unlock()
		return err
	}
	e.walBytesSinceCkpt = 0
	e.mutationsSinceCkpt = false
	e.lastCheckpoint = time.Now()
	e.pinMu.Unlock()
	return nil
}

func (e *Engine) finishClose(req request) {
	e.closed.Store(true)
	e.waitPins()
	_ = e.log.Sync()
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

// Get reads a statement snapshot at committedSequence without joining the writer queue.
func (e *Engine) Get(key []byte) ([]byte, error) {
	return e.GetAt(key, e.committedSequence.Load())
}

// GetAt returns the value visible at snapshotSequence. Pins for the call duration
// (a future Tx can hold an additional long-lived Pin on the same sequence).
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

// Has is a statement-snapshot existence check (same path as Get).
func (e *Engine) Has(key []byte) (bool, error) {
	return e.HasAt(key, e.committedSequence.Load())
}

// HasAt reports whether key exists at snapshotSequence.
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
