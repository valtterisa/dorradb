package dorradb

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
)

func init() {
	sql.Register("dorradb", &sqlDriver{})
}

type sqlDriver struct{}

func (d *sqlDriver) Open(name string) (driver.Conn, error) {
	db, err := Open(name)
	if err != nil {
		return nil, err
	}
	return &sqlConn{db: db}, nil
}

type sqlConn struct{ db *DB }

func (c *sqlConn) Prepare(query string) (driver.Stmt, error) {
	s, err := c.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &sqlStmt{stmt: s}, nil
}

func (c *sqlConn) Close() error { return c.db.Close() }

func (c *sqlConn) Begin() (driver.Tx, error) {
	tx, err := c.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &sqlTx{tx: tx}, nil
}

type sqlTx struct{ tx *Tx }

func (t *sqlTx) Commit() error   { return t.tx.Commit() }
func (t *sqlTx) Rollback() error { return t.tx.Rollback() }

type sqlStmt struct{ stmt *Stmt }

func (s *sqlStmt) Close() error  { return nil }
func (s *sqlStmt) NumInput() int { return -1 }

func (s *sqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	a := make([]any, len(args))
	for i, v := range args {
		a[i] = v
	}
	res, err := s.stmt.Exec(a...)
	if err != nil {
		return nil, err
	}
	return driverResult{res}, nil
}

func (s *sqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	a := make([]any, len(args))
	for i, v := range args {
		a[i] = v
	}
	rows, err := s.stmt.Query(a...)
	if err != nil {
		return nil, err
	}
	return &sqlRows{rows: rows, cols: rows.Columns()}, nil
}

type driverResult struct{ r Result }

func (r driverResult) LastInsertId() (int64, error) {
	return 0, errors.New("dorradb: LastInsertId not supported")
}
func (r driverResult) RowsAffected() (int64, error) { return r.r.RowsAffected() }

type sqlRows struct {
	rows *Rows
	cols []string
}

func (r *sqlRows) Columns() []string { return r.cols }
func (r *sqlRows) Close() error      { return r.rows.Close() }

func (r *sqlRows) Next(dest []driver.Value) error {
	if !r.rows.Next() {
		return io.EOF
	}
	args := make([]any, len(dest))
	for i := range args {
		var v any
		args[i] = &v
	}
	if err := r.rows.Scan(args...); err != nil {
		return err
	}
	for i := range dest {
		dest[i] = *(args[i].(*any))
	}
	return nil
}
