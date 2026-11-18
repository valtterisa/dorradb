package dorradb

import (
	"errors"
	"fmt"

	"dorradb/internal/sql"
)

// Result summarizes a completed Exec.
type Result struct {
	rowsAffected int64
}

func (r Result) RowsAffected() (int64, error) { return r.rowsAffected, nil }

// Rows is a SELECT result set. When obtained from DB.Query, Close releases the
// underlying read transaction.
type Rows struct {
	cols []string
	data [][]sql.Value
	i    int
	tx   *Tx // non-nil for DB.Query AUTOCOMMIT reads
}

func (r *Rows) Columns() []string {
	if r == nil {
		return nil
	}
	return r.cols
}

func (r *Rows) Next() bool {
	if r == nil || r.i+1 >= len(r.data) {
		if r != nil {
			r.i = len(r.data)
		}
		return false
	}
	r.i++
	return true
}

func (r *Rows) Scan(dest ...any) error {
	if r == nil || r.i < 0 || r.i >= len(r.data) {
		return errors.New("sql: no rows")
	}
	vals := r.data[r.i]
	if len(dest) != len(vals) {
		return fmt.Errorf("sql: Scan expected %d destinations, got %d", len(vals), len(dest))
	}
	for i, v := range vals {
		if err := assignScan(dest[i], v); err != nil {
			return err
		}
	}
	return nil
}

func (r *Rows) Close() error {
	if r == nil {
		return nil
	}
	if r.tx != nil {
		_ = r.tx.Rollback()
		r.tx = nil
	}
	return nil
}

func assignScan(dest any, v sql.Value) error {
	if v.Null {
		switch d := dest.(type) {
		case *any:
			*d = nil
		case *[]byte:
			*d = nil
		case *string:
			*d = ""
		case *int64:
			*d = 0
		default:
			return fmt.Errorf("sql: cannot scan NULL into %T", dest)
		}
		return nil
	}
	switch d := dest.(type) {
	case *any:
		switch v.Type {
		case sql.TypeInteger:
			*d = v.Int
		case sql.TypeText:
			*d = v.Text
		case sql.TypeBlob:
			*d = append([]byte(nil), v.Blob...)
		}
	case *int64:
		if v.Type != sql.TypeInteger {
			return fmt.Errorf("sql: type mismatch")
		}
		*d = v.Int
	case *string:
		switch v.Type {
		case sql.TypeText:
			*d = v.Text
		case sql.TypeBlob:
			*d = string(v.Blob)
		case sql.TypeInteger:
			*d = fmt.Sprintf("%d", v.Int)
		}
	case *[]byte:
		switch v.Type {
		case sql.TypeBlob:
			*d = append([]byte(nil), v.Blob...)
		case sql.TypeText:
			*d = []byte(v.Text)
		case sql.TypeInteger:
			*d = []byte(fmt.Sprintf("%d", v.Int))
		}
	default:
		return fmt.Errorf("sql: unsupported Scan destination %T", dest)
	}
	return nil
}

// Stmt is a prepared statement.
type Stmt struct {
	db   *DB
	stmt sql.Stmt
}

// Exec runs sql with optional args (AUTOCOMMIT write). Use Query for SELECT.
func (db *DB) Exec(query string, args ...any) (Result, error) {
	stmt, err := sql.Parse(query)
	if err != nil {
		return Result{}, err
	}
	if _, ok := stmt.(*sql.SelectStmt); ok {
		return Result{}, errors.New("dorradb: use Query for SELECT")
	}
	tx, err := db.Begin(true)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()
	n, err := tx.runExec(stmt, args)
	if err != nil {
		return Result{}, err
	}
	if err := tx.Commit(); err != nil {
		return Result{}, err
	}
	return Result{rowsAffected: n}, nil
}

// Query runs a SELECT (AUTOCOMMIT read). Caller must Close the Rows.
func (db *DB) Query(query string, args ...any) (*Rows, error) {
	stmt, err := sql.Parse(query)
	if err != nil {
		return nil, err
	}
	tx, err := db.Begin(false)
	if err != nil {
		return nil, err
	}
	rows, err := tx.runQuery(stmt, args)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	rows.tx = tx
	return rows, nil
}

// Prepare parses sql once.
func (db *DB) Prepare(query string) (*Stmt, error) {
	stmt, err := sql.Parse(query)
	if err != nil {
		return nil, err
	}
	return &Stmt{db: db, stmt: stmt}, nil
}

func (s *Stmt) Exec(args ...any) (Result, error) {
	if _, ok := s.stmt.(*sql.SelectStmt); ok {
		return Result{}, errors.New("dorradb: use Query for SELECT")
	}
	tx, err := s.db.Begin(true)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()
	n, err := tx.runExec(s.stmt, args)
	if err != nil {
		return Result{}, err
	}
	if err := tx.Commit(); err != nil {
		return Result{}, err
	}
	return Result{rowsAffected: n}, nil
}

func (s *Stmt) Query(args ...any) (*Rows, error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, err
	}
	rows, err := tx.runQuery(s.stmt, args)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	rows.tx = tx
	return rows, nil
}

// Exec runs sql inside this transaction.
func (tx *Tx) Exec(query string, args ...any) (Result, error) {
	if tx.done {
		return Result{}, ErrTxClosed
	}
	if !tx.writable {
		return Result{}, ErrTxNotWritable
	}
	stmt, err := sql.Parse(query)
	if err != nil {
		return Result{}, err
	}
	if _, ok := stmt.(*sql.SelectStmt); ok {
		return Result{}, errors.New("dorradb: use Query for SELECT")
	}
	n, err := tx.runExec(stmt, args)
	if err != nil {
		return Result{}, err
	}
	return Result{rowsAffected: n}, nil
}

// Query runs a SELECT inside this transaction.
func (tx *Tx) Query(query string, args ...any) (*Rows, error) {
	if tx.done {
		return nil, ErrTxClosed
	}
	stmt, err := sql.Parse(query)
	if err != nil {
		return nil, err
	}
	return tx.runQuery(stmt, args)
}

