package dorradb

import "dorradb/internal/engine"

func (tx *Tx) getTable(table string, key []byte) ([]byte, error) {
	if op, ok := tx.bufGet(table, string(key)); ok {
		if op.deleted {
			return nil, engine.ErrNotFound
		}
		return append([]byte(nil), op.value...), nil
	}
	return tx.db.eng.GetAtTable(table, key, tx.snapSeq)
}

func (tx *Tx) hasTable(table string, key []byte) (bool, error) {
	if op, ok := tx.bufGet(table, string(key)); ok {
		return !op.deleted, nil
	}
	ok, err := tx.db.eng.HasAtTable(table, key, tx.snapSeq)
	return ok, mapErr(err)
}

func (tx *Tx) setTable(table string, key, value []byte) error {
	if len(value) > maxValueBytes {
		return ErrTooLarge
	}
	tx.bufSet(table, string(key), bufferedOp{value: append([]byte(nil), value...)})
	return nil
}

func (tx *Tx) deleteTable(table string, key []byte) error {
	tx.bufSet(table, string(key), bufferedOp{deleted: true})
	return nil
}
