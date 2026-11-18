package sql_test

import (
	"testing"

	"dorradb/internal/sql"
)

func TestParseCreateInsertSelect(t *testing.T) {
	s, err := sql.Parse(`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	c, ok := s.(*sql.CreateTableStmt)
	if !ok || !c.IfNotExists || len(c.Columns) != 2 {
		t.Fatalf("%#v", s)
	}

	s, err = sql.Parse(`INSERT OR REPLACE INTO t (id, name) VALUES (?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	ins := s.(*sql.InsertStmt)
	if !ins.OrReplace || len(ins.Values) != 2 {
		t.Fatalf("%#v", ins)
	}

	s, err = sql.Parse(`SELECT * FROM t WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	sel := s.(*sql.SelectStmt)
	if !sel.Star || sel.Where == nil {
		t.Fatalf("%#v", sel)
	}
}

func TestParseRejectJoin(t *testing.T) {
	_, err := sql.Parse(`SELECT * FROM a JOIN b`)
	if err == nil {
		t.Fatal("expected error")
	}
}
