package dorradb

import (
	"errors"
	"fmt"
	"strings"

	"dorradb/internal/engine"
	"dorradb/internal/sql"
)

func (tx *Tx) runExec(stmt sql.Stmt, args []any) (int64, error) {
	switch s := stmt.(type) {
	case *sql.CreateTableStmt:
		return 0, tx.execCreate(s)
	case *sql.DropTableStmt:
		return 0, tx.execDrop(s)
	case *sql.InsertStmt:
		return tx.execInsert(s, args)
	case *sql.UpdateStmt:
		return tx.execUpdate(s, args)
	case *sql.DeleteStmt:
		return tx.execDelete(s, args)
	default:
		return 0, fmt.Errorf("sql: unsupported statement")
	}
}

func (tx *Tx) runQuery(stmt sql.Stmt, args []any) (*Rows, error) {
	s, ok := stmt.(*sql.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("sql: not a query")
	}
	return tx.execSelect(s, args)
}

func (tx *Tx) execCreate(s *sql.CreateTableStmt) error {
	cols := make([]sql.Column, len(s.Columns))
	for i, c := range s.Columns {
		cols[i] = sql.Column{Name: c.Name, Type: c.Type, PrimaryKey: c.PrimaryKey, NotNull: c.NotNull}
	}
	tx.ddls = append(tx.ddls, &engine.DDL{
		Create: true, IfNotExists: s.IfNotExists, Name: s.Name, Columns: cols,
	})
	return nil
}

func (tx *Tx) execDrop(s *sql.DropTableStmt) error {
	tx.ddls = append(tx.ddls, &engine.DDL{
		Drop: true, IfExists: s.IfExists, Name: s.Name,
	})
	return nil
}

func (tx *Tx) execInsert(s *sql.InsertStmt, args []any) (int64, error) {
	tab, ok := tx.db.eng.Catalog().Get(s.Table)
	if !ok {
		return 0, ErrTableNotFound
	}
	if err := needArgs(countParams(s.Values...), args); err != nil {
		return 0, err
	}
	colNames := s.Columns
	if len(colNames) == 0 {
		colNames = make([]string, len(tab.Columns))
		for i, c := range tab.Columns {
			colNames[i] = c.Name
		}
	}
	if len(colNames) != len(s.Values) {
		return 0, fmt.Errorf("sql: column/value count mismatch")
	}
	vals := make([]sql.Value, len(tab.Columns))
	set := make([]bool, len(tab.Columns))
	for i, name := range colNames {
		_, idx, ok := tab.Col(name)
		if !ok {
			return 0, fmt.Errorf("sql: unknown column %q", name)
		}
		v, err := evalExpr(s.Values[i], args)
		if err != nil {
			return 0, err
		}
		vals[idx] = v
		set[idx] = true
	}
	for i, c := range tab.Columns {
		if !set[i] {
			if c.NotNull || c.PrimaryKey {
				return 0, ErrConstraint
			}
			vals[i] = sql.NullValue()
		}
	}
	pk := tab.PK()
	_, pkIdx, _ := tab.Col(pk.Name)
	pkKey, err := sql.EncodePK(pk, vals[pkIdx])
	if err != nil {
		return 0, err
	}
	if !s.OrReplace {
		exists, err := tx.hasTable(tab.Name, pkKey)
		if err != nil {
			return 0, err
		}
		if exists {
			return 0, ErrConstraint
		}
	}
	nonVals := make([]sql.Value, 0, len(tab.Columns)-1)
	for i, c := range tab.Columns {
		if !c.PrimaryKey {
			nonVals = append(nonVals, vals[i])
		}
	}
	rec, err := sql.EncodeRecord(tab.NonPK(), nonVals)
	if err != nil {
		return 0, err
	}
	if err := tx.setTable(tab.Name, pkKey, rec); err != nil {
		return 0, err
	}
	return 1, nil
}

func (tx *Tx) execUpdate(s *sql.UpdateStmt, args []any) (int64, error) {
	tab, ok := tx.db.eng.Catalog().Get(s.Table)
	if !ok {
		return 0, ErrTableNotFound
	}
	n := 0
	for _, a := range s.Sets {
		n += countParams(a.Value)
	}
	if s.Where != nil {
		n += countParams(s.Where.Value)
	}
	if err := needArgs(n, args); err != nil {
		return 0, err
	}
	pk := tab.PK()
	if !strings.EqualFold(s.Where.Column, pk.Name) {
		return 0, fmt.Errorf("sql: WHERE must be on primary key %s", pk.Name)
	}
	pkVal, err := evalExpr(s.Where.Value, args)
	if err != nil {
		return 0, err
	}
	pkKey, err := sql.EncodePK(pk, pkVal)
	if err != nil {
		return 0, err
	}
	rec, err := tx.getTable(tab.Name, pkKey)
	if err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	full, err := sql.Project(tab, pkKey, rec)
	if err != nil {
		return 0, err
	}
	for _, a := range s.Sets {
		col, idx, ok := tab.Col(a.Column)
		if !ok {
			return 0, fmt.Errorf("sql: unknown column %q", a.Column)
		}
		if col.PrimaryKey {
			return 0, fmt.Errorf("sql: cannot UPDATE primary key")
		}
		v, err := evalExpr(a.Value, args)
		if err != nil {
			return 0, err
		}
		full[idx] = v
	}
	nonVals := make([]sql.Value, 0, len(tab.Columns)-1)
	for i, c := range tab.Columns {
		if !c.PrimaryKey {
			nonVals = append(nonVals, full[i])
		}
	}
	out, err := sql.EncodeRecord(tab.NonPK(), nonVals)
	if err != nil {
		return 0, err
	}
	if err := tx.setTable(tab.Name, pkKey, out); err != nil {
		return 0, err
	}
	return 1, nil
}

func (tx *Tx) execDelete(s *sql.DeleteStmt, args []any) (int64, error) {
	tab, ok := tx.db.eng.Catalog().Get(s.Table)
	if !ok {
		return 0, ErrTableNotFound
	}
	if err := needArgs(countParams(s.Where.Value), args); err != nil {
		return 0, err
	}
	pk := tab.PK()
	if !strings.EqualFold(s.Where.Column, pk.Name) {
		return 0, fmt.Errorf("sql: WHERE must be on primary key %s", pk.Name)
	}
	pkVal, err := evalExpr(s.Where.Value, args)
	if err != nil {
		return 0, err
	}
	pkKey, err := sql.EncodePK(pk, pkVal)
	if err != nil {
		return 0, err
	}
	exists, err := tx.hasTable(tab.Name, pkKey)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	if err := tx.deleteTable(tab.Name, pkKey); err != nil {
		return 0, err
	}
	return 1, nil
}

func (tx *Tx) execSelect(s *sql.SelectStmt, args []any) (*Rows, error) {
	tab, ok := tx.db.eng.Catalog().Get(s.Table)
	if !ok {
		return nil, ErrTableNotFound
	}
	n := 0
	if s.Where != nil {
		n = countParams(s.Where.Value)
	}
	if err := needArgs(n, args); err != nil {
		return nil, err
	}
	projNames := s.Columns
	if s.Star {
		projNames = make([]string, len(tab.Columns))
		for i, c := range tab.Columns {
			projNames[i] = c.Name
		}
	}
	projIdx := make([]int, len(projNames))
	for i, name := range projNames {
		_, idx, ok := tab.Col(name)
		if !ok {
			return nil, fmt.Errorf("sql: unknown column %q", name)
		}
		projIdx[i] = idx
	}
	out := &Rows{cols: projNames, i: -1}

	appendRow := func(full []sql.Value) {
		rowVals := make([]sql.Value, len(projIdx))
		for i, idx := range projIdx {
			rowVals[i] = full[idx]
		}
		out.data = append(out.data, rowVals)
	}

	if s.Where != nil {
		pk := tab.PK()
		if !strings.EqualFold(s.Where.Column, pk.Name) {
			return nil, fmt.Errorf("sql: WHERE must be on primary key %s", pk.Name)
		}
		pkVal, err := evalExpr(s.Where.Value, args)
		if err != nil {
			return nil, err
		}
		pkKey, err := sql.EncodePK(pk, pkVal)
		if err != nil {
			return nil, err
		}
		rec, err := tx.getTable(tab.Name, pkKey)
		if err != nil {
			if errors.Is(err, engine.ErrNotFound) {
				return out, nil
			}
			return nil, err
		}
		full, err := sql.Project(tab, pkKey, rec)
		if err != nil {
			return nil, err
		}
		appendRow(full)
		return out, nil
	}

	sc, err := tx.db.eng.ScanAt(tab.Name, tx.snapSeq, nil)
	if err != nil {
		return nil, mapErr(err)
	}
	defer sc.Close()
	for sc.Valid() {
		full, err := sql.Project(tab, sc.Key(), sc.Value())
		if err != nil {
			return nil, err
		}
		appendRow(full)
		sc.Next()
	}
	return out, sc.Err()
}

func evalExpr(e sql.Expr, args []any) (sql.Value, error) {
	switch v := e.(type) {
	case sql.ParamExpr:
		if v.Index < 1 || v.Index > len(args) {
			return sql.Value{}, fmt.Errorf("sql: wrong number of arguments")
		}
		return valueFromArg(args[v.Index-1])
	case sql.IntExpr:
		return sql.IntValue(v.Value), nil
	case sql.TextExpr:
		return sql.TextValue(v.Value), nil
	case sql.BlobExpr:
		return sql.BlobValue(v.Value), nil
	case sql.NullExpr:
		return sql.NullValue(), nil
	default:
		return sql.Value{}, fmt.Errorf("sql: bad expression")
	}
}

func valueFromArg(a any) (sql.Value, error) {
	if a == nil {
		return sql.NullValue(), nil
	}
	switch v := a.(type) {
	case int:
		return sql.IntValue(int64(v)), nil
	case int64:
		return sql.IntValue(v), nil
	case int32:
		return sql.IntValue(int64(v)), nil
	case string:
		return sql.TextValue(v), nil
	case []byte:
		return sql.BlobValue(v), nil
	case bool:
		if v {
			return sql.IntValue(1), nil
		}
		return sql.IntValue(0), nil
	default:
		return sql.Value{}, fmt.Errorf("sql: unsupported arg type %T", a)
	}
}

func countParams(exprs ...sql.Expr) int {
	n := 0
	for _, e := range exprs {
		if _, ok := e.(sql.ParamExpr); ok {
			n++
		}
	}
	return n
}

func needArgs(want int, args []any) error {
	if len(args) != want {
		return fmt.Errorf("sql: wrong number of arguments: want %d got %d", want, len(args))
	}
	return nil
}
