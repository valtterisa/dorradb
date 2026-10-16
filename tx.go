package dorradb

import (
	"dorradb/internal/engine"
)

// Tx is a read-only or read-write transaction. Reads see a snapshot taken at
// Begin plus this tx's own buffered writes. Writes are applied atomically on
// Commit; Rollback discards them.
type Tx struct {
	db       *DB
	snapSeq  uint64
	writable bool
	done     bool
	writes   map[string]bufferedOp
}

type bufferedOp struct {
	deleted bool
	value   []byte
}

// Begin starts a transaction. If writable is true, only one writable tx may be
// active at a time (callers wait on a mutex). Read-only txs may run concurrently.
// Always defer Rollback (safe after Commit).
func (db *DB) Begin(writable bool) (*Tx, error) {
	if writable {
		db.writeMu.Lock()
	}
	if err := mapErr(db.eng.Err()); err != nil {
		if writable {
			db.writeMu.Unlock()
		}
		return nil, err
	}
	snap := db.eng.CommittedSequence()
	if !db.eng.Pin(snap) {
		if writable {
			db.writeMu.Unlock()
		}
		if err := mapErr(db.eng.Err()); err != nil {
			return nil, err
		}
		return nil, ErrClosed
	}
	return &Tx{
		db:       db,
		snapSeq:  snap,
		writable: writable,
		writes:   make(map[string]bufferedOp),
	}, nil
}

// Get returns a copy of the value for key, or ErrNotFound.
func (tx *Tx) Get(key []byte) ([]byte, error) {
	if err := checkKey(key); err != nil {
		return nil, err
	}
	if tx.done {
		return nil, ErrTxClosed
	}
	if op, ok := tx.writes[string(key)]; ok {
		if op.deleted {
			return nil, ErrNotFound
		}
		return append([]byte(nil), op.value...), nil
	}
	val, err := tx.db.eng.GetAt(key, tx.snapSeq)
	return val, mapErr(err)
}

// Has reports whether key exists at the tx snapshot (including buffered writes).
func (tx *Tx) Has(key []byte) (bool, error) {
	if err := checkKey(key); err != nil {
		return false, err
	}
	if tx.done {
		return false, ErrTxClosed
	}
	if op, ok := tx.writes[string(key)]; ok {
		return !op.deleted, nil
	}
	ok, err := tx.db.eng.HasAt(key, tx.snapSeq)
	return ok, mapErr(err)
}

// Set buffers a create/overwrite. Durable only after Commit.
func (tx *Tx) Set(key, value []byte) error {
	if err := checkKey(key); err != nil {
		return err
	}
	if len(value) > maxValueBytes {
		return ErrTooLarge
	}
	if tx.done {
		return ErrTxClosed
	}
	if !tx.writable {
		return ErrTxNotWritable
	}
	tx.writes[string(key)] = bufferedOp{
		value: append([]byte(nil), value...),
	}
	return nil
}

// Delete buffers a removal. Missing keys are not an error. Durable only after Commit.
func (tx *Tx) Delete(key []byte) error {
	if err := checkKey(key); err != nil {
		return err
	}
	if tx.done {
		return ErrTxClosed
	}
	if !tx.writable {
		return ErrTxNotWritable
	}
	tx.writes[string(key)] = bufferedOp{deleted: true}
	return nil
}

// Commit applies buffered writes atomically and releases the snapshot pin.
// Read-only commits only release the pin. After Commit/Rollback, further use
// returns ErrTxClosed.
func (tx *Tx) Commit() error {
	if tx.done {
		return ErrTxClosed
	}
	tx.done = true

	ops := make([]engine.Mutation, 0, len(tx.writes))
	for k, op := range tx.writes {
		ops = append(ops, engine.Mutation{
			Delete: op.deleted,
			Key:    []byte(k),
			Value:  op.value,
		})
	}
	tx.writes = nil

	// Release the snapshot pin before Commit so vacuum can proceed without
	// deadlocking against this tx (vacuum skips while pins remain).
	tx.db.eng.Unpin(tx.snapSeq)

	var err error
	if tx.writable && len(ops) > 0 {
		err = mapErr(tx.db.eng.Commit(ops))
	}
	if tx.writable {
		tx.db.writeMu.Unlock()
	}
	return err
}

// Rollback discards buffered writes and releases the snapshot pin.
// Safe (no-op) after Commit or a prior Rollback.
func (tx *Tx) Rollback() error {
	if tx.done {
		return nil
	}
	tx.done = true
	tx.writes = nil
	tx.db.eng.Unpin(tx.snapSeq)
	if tx.writable {
		tx.db.writeMu.Unlock()
	}
	return nil
}
