package engine

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"time"

	"dorradb/internal/encode"
	"dorradb/internal/index"
	"dorradb/internal/storage"
	"dorradb/internal/vfs"
	"dorradb/internal/wal"
)

type Options struct {
	Path                  string
	FS                    vfs.FS
	WALCheckpointBytes    int64
	WALCheckpointInterval time.Duration
	SyncWAL               bool
	GroupCommitMaxOps     int
	GroupCommitMaxWait    time.Duration
	MaxKeyBytes           int
	MaxValueBytes         int
}

type Stats struct {
	Path            string
	Seq             uint64
	LiveKeys        int
	MainBytes       int64
	WALBytes        int64
	ApproxDeadBytes int64
	LastCheckpoint  time.Time
}

type opKind int

const (
	opGet opKind = iota
	opHas
	opSet
	opDelete
	opCheckpoint
	opStats
	opClose
)

type request struct {
	kind  opKind
	key   []byte
	value []byte
	ctx   context.Context
	reply chan reply
}

type reply struct {
	value []byte
	has   bool
	stats Stats
	err   error
}

type Engine struct {
	opts Options
	fs   vfs.FS
	lk   vfs.Lock
	main *storage.Main
	log  *wal.Log
	idx  *index.Index

	seq                uint64
	walBytesSinceCkpt  int64
	lastCheckpoint     time.Time
	mutationsSinceCkpt bool
	ckptInFlight       bool

	reqCh  chan request
	closed atomic.Bool
	done   chan struct{}
}

var (
	ErrNotFound = errors.New("engine: not found")
	ErrClosed   = errors.New("engine: closed")
)

func Open(opts Options) (*Engine, error) {
	if opts.Path == "" {
		return nil, errors.New("engine: empty path")
	}
	fs := opts.FS
	if fs == nil {
		fs = vfs.OS()
	}
	dir := filepath.Dir(opts.Path)
	if dir != "." && dir != "" {
		if err := fs.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	lk, err := fs.Lock(opts.Path + ".lock")
	if err != nil {
		return nil, err
	}

	main, err := storage.Open(fs, opts.Path, true)
	if err != nil {
		_ = lk.Unlock()
		return nil, err
	}
	log, err := wal.Open(fs, opts.Path, true)
	if err != nil {
		_ = main.Close()
		_ = lk.Unlock()
		return nil, err
	}

	e := &Engine{
		opts:           opts,
		fs:             fs,
		lk:             lk,
		main:           main,
		log:            log,
		idx:            index.New(),
		lastCheckpoint: time.Now(),
		reqCh:          make(chan request, 256),
		done:           make(chan struct{}),
	}

	if err := e.recover(); err != nil {
		e.cleanup()
		return nil, err
	}

	go e.loop()
	return e, nil
}

func (e *Engine) recover() error {
	if err := e.main.ScanIndex(e.idx); err != nil {
		return err
	}
	maxMainSeq := e.idx.MaxSeq()
	if hseq := e.main.Header().Seq; hseq > maxMainSeq {
		maxMainSeq = hseq
	}
	e.seq = maxMainSeq

	from := int64(e.main.Header().WalCheckpointOffset)
	if from < encode.HeaderSize {
		from = encode.HeaderSize
	}
	err := e.log.Replay(from, func(fv encode.FrameView) error {
		rec := fv.Record
		if rec.Seq > e.seq {
			e.seq = rec.Seq
		}
		if rec.Seq <= maxMainSeq {
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
		case opGet:
			e.handleGet(req)
		case opHas:
			e.handleHas(req)
		case opStats:
			e.handleStats(req)
		case opCheckpoint:
			e.handleCheckpoint(req)
		case opSet, opDelete:
			e.handleMutations(req)
		}
	}
}

func (e *Engine) handleGet(req request) {
	loc, ok := e.idx.Get(req.key)
	if !ok || loc.Deleted {
		req.reply <- reply{err: ErrNotFound}
		return
	}
	val, err := e.main.ReadValue(loc)
	if err != nil {
		req.reply <- reply{err: err}
		return
	}
	req.reply <- reply{value: append([]byte(nil), val...)}
}

func (e *Engine) handleHas(req request) {
	loc, ok := e.idx.Get(req.key)
	req.reply <- reply{has: ok && !loc.Deleted}
}

func (e *Engine) handleStats(req request) {
	live := e.idx.ApproxLiveBytes()
	mainSize := e.main.Size()
	dead := mainSize - encode.HeaderSize - live
	if dead < 0 {
		dead = 0
	}
	req.reply <- reply{stats: Stats{
		Path:            e.opts.Path,
		Seq:             e.seq,
		LiveKeys:        e.idx.LiveCount(),
		MainBytes:       mainSize,
		WALBytes:        e.log.Size(),
		ApproxDeadBytes: dead,
		LastCheckpoint:  e.lastCheckpoint,
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
			case opClose:
				e.applyGroup(group)
				e.finishClose(req)
				return
			case opGet:
				e.handleGet(req)
			case opHas:
				e.handleHas(req)
			case opStats:
				e.handleStats(req)
			case opCheckpoint:
				e.applyGroup(group)
				e.handleCheckpoint(req)
				return
			}
		case <-timer.C:
			break gather
		}
	}
	e.applyGroup(group)
	e.maybeCheckpoint()
}

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
		e.seq++
		rec := encode.Record{Seq: e.seq, Key: req.key}
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
	for _, it := range items {
		loc, err := e.main.Append(it.rec)
		if err != nil {
			it.req.reply <- reply{err: err}
			continue
		}
		e.idx.Apply(it.rec.Key, loc)
		e.walBytesSinceCkpt += it.frameLen
		e.mutationsSinceCkpt = true
		it.req.reply <- reply{}
	}
}

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

func (e *Engine) handleCheckpoint(req request) {
	if req.ctx != nil {
		select {
		case <-req.ctx.Done():
			req.reply <- reply{err: req.ctx.Err()}
			return
		default:
		}
	}
	req.reply <- reply{err: e.compact()}
}

func (e *Engine) compact() error {
	e.ckptInFlight = true
	defer func() { e.ckptInFlight = false }()

	var records []encode.Record
	err := e.idx.ForEachLive(func(key string, loc index.Loc) error {
		val, err := e.main.ReadValue(loc)
		if err != nil {
			return err
		}
		records = append(records, encode.Record{
			Seq:   loc.Seq,
			Op:    encode.OpSet,
			Key:   []byte(key),
			Value: val,
		})
		return nil
	})
	if err != nil {
		return err
	}

	tmp := e.opts.Path + ".tmp"
	h := encode.Header{
		FileType:            encode.FileTypeMain,
		FormatVersion:       encode.FormatVersion,
		Seq:                 e.seq,
		WalCheckpointOffset: encode.HeaderSize,
	}
	newSize, err := storage.WriteCompactFile(e.fs, tmp, h, records)
	if err != nil {
		_ = e.fs.Remove(tmp)
		return err
	}
	if err := e.main.ReplaceWithCompact(tmp, h, newSize); err != nil {
		_ = e.fs.Remove(tmp)
		return err
	}
	if err := e.log.TruncateReset(); err != nil {
		return err
	}
	if err := e.main.ScanIndex(e.idx); err != nil {
		return err
	}
	e.walBytesSinceCkpt = 0
	e.mutationsSinceCkpt = false
	e.lastCheckpoint = time.Now()
	return nil
}

func (e *Engine) finishClose(req request) {
	e.closed.Store(true)
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

func (e *Engine) Get(key []byte) ([]byte, error) {
	r := e.call(request{kind: opGet, key: key})
	if r.err != nil {
		return nil, r.err
	}
	return r.value, nil
}

func (e *Engine) Has(key []byte) (bool, error) {
	r := e.call(request{kind: opHas, key: key})
	return r.has, r.err
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

func (e *Engine) Checkpoint(ctx context.Context) error {
	return e.call(request{kind: opCheckpoint, ctx: ctx}).err
}

func (e *Engine) Stats() Stats {
	return e.call(request{kind: opStats}).stats
}

func (e *Engine) Close() error {
	r := e.call(request{kind: opClose})
	<-e.done
	return r.err
}
