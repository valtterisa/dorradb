package dorradb

import (
	"dorradb/internal/engine"
)

// Tx is a read-only or read-write transaction. Reads see a snapshot taken at
// Begin plus this tx's own buffered writes. Writes are applied atomically on
// Commit; Rollback discards them. Use Exec/Query for SQL.
type Tx struct {
	db       *DB
	snapSeq  uint64
	writable bool
	done     bool
	writes   map[string]map[string]bufferedOp // table → pk → op
	ddls     []*engine.DDL
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
	snap, ok := db.eng.PinLatest()
	if !ok {
		if writable {
			db.writeMu.Unlock()
		}
		return nil, ErrClosed
	}
	return &Tx{
		db:       db,
		snapSeq:  snap,
		writable: writable,
		writes:   make(map[string]map[string]bufferedOp),
	}, nil
}

func (tx *Tx) bufGet(table string, key string) (bufferedOp, bool) {
	m, ok := tx.writes[table]
	if !ok {
		return bufferedOp{}, false
	}
	op, ok := m[key]
	return op, ok
}

func (tx *Tx) bufSet(table string, key string, op bufferedOp) {
	m, ok := tx.writes[table]
	if !ok {
		m = make(map[string]bufferedOp)
		tx.writes[table] = m
	}
	m[key] = op
}

// Commit applies buffered writes atomically and releases the snapshot pin.
// Read-only commits only release the pin. After Commit/Rollback, further use
// returns ErrTxClosed.
func (tx *Tx) Commit() error {
	if tx.done {
		return ErrTxClosed
	}
	tx.done = true

	ops := make([]engine.Mutation, 0, 8)
	for _, d := range tx.ddls {
		ops = append(ops, engine.Mutation{DDL: d})
	}
	for table, m := range tx.writes {
		for k, op := range m {
			ops = append(ops, engine.Mutation{
				Delete: op.deleted,
				Table:  table,
				Key:    []byte(k),
				Value:  op.value,
			})
		}
	}
	tx.writes = nil
	tx.ddls = nil

	// Release the snapshot pin before ApplyBatch so checkpoint/vacuum on the
	// writer can waitPins without deadlocking against this tx.
	tx.db.eng.Unpin(tx.snapSeq)

	var err error
	if tx.writable && len(ops) > 0 {
		err = mapErr(tx.db.eng.ApplyBatch(ops))
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
	tx.ddls = nil
	tx.db.eng.Unpin(tx.snapSeq)
	if tx.writable {
		tx.db.writeMu.Unlock()
	}
	return nil
}
