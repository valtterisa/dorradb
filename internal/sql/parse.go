// package sql is a recursive-descent parser for DorraDB SQL.
package sql

import (
	"encoding/hex"
	"fmt"
	"strconv"
)

type Parser struct {
	lx     *Lexer
	tok    Token
	params int
}

func Parse(sql string) (Stmt, error) {
	p := &Parser{lx: newLexer(sql)}
	if err := p.next(); err != nil {
		return nil, err
	}
	stmt, err := p.parseStmt()
	if err != nil {
		return nil, err
	}
	if p.tok.Kind != EOF && p.tok.Kind != Semi {
		return nil, fmt.Errorf("sql: unexpected token %q after statement", p.tok.Lit)
	}
	if p.tok.Kind == Semi {
		if err := p.next(); err != nil {
			return nil, err
		}
		if p.tok.Kind != EOF {
			return nil, fmt.Errorf("sql: multiple statements not supported")
		}
	}
	return stmt, nil
}

func (p *Parser) next() error {
	t, err := p.lx.Next()
	if err != nil {
		return err
	}
	p.tok = t
	return nil
}

func (p *Parser) parseStmt() (Stmt, error) {
	if p.tok.Kind != Keyword {
		return nil, fmt.Errorf("sql: expected statement keyword, got %q", p.tok.Lit)
	}
	switch p.tok.Keyword {
	case "CREATE":
		return p.parseCreate()
	case "DROP":
		return p.parseDrop()
	case "INSERT":
		return p.parseInsert()
	case "UPDATE":
		return p.parseUpdate()
	case "DELETE":
		return p.parseDelete()
	case "SELECT":
		return p.parseSelect()
	default:
		return nil, fmt.Errorf("sql: unsupported statement %s", p.tok.Keyword)
	}
}

func (p *Parser) expectKeyword(kw string) error {
	if p.tok.Kind != Keyword || p.tok.Keyword != kw {
		return fmt.Errorf("sql: expected %s, got %q", kw, p.tok.Lit)
	}
	return p.next()
}

func (p *Parser) expect(kind Kind) error {
	if p.tok.Kind != kind {
		return fmt.Errorf("sql: expected %v, got %q", kind, p.tok.Lit)
	}
	return p.next()
}

func (p *Parser) parseIdent() (string, error) {
	if p.tok.Kind != Ident {
		return "", fmt.Errorf("sql: expected identifier, got %q", p.tok.Lit)
	}
	name := p.tok.Lit
	if err := p.next(); err != nil {
		return "", err
	}
	return name, nil
}

func (p *Parser) parseCreate() (Stmt, error) {
	if err := p.next(); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	ifNotExists := false
	if p.tok.Kind == Keyword && p.tok.Keyword == "IF" {
		if err := p.next(); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifNotExists = true
	}
	name, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expect(LParen); err != nil {
		return nil, err
	}
	var cols []ColumnDef
	for {
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		if p.tok.Kind == Comma {
			if err := p.next(); err != nil {
				return nil, err
			}
			continue
		}
		break
	}
	if err := p.expect(RParen); err != nil {
		return nil, err
	}
	pk := 0
	for _, c := range cols {
		if c.PrimaryKey {
			pk++
		}
	}
	if pk != 1 {
		return nil, fmt.Errorf("sql: CREATE TABLE requires exactly one PRIMARY KEY column")
	}
	return &CreateTableStmt{Name: name, IfNotExists: ifNotExists, Columns: cols}, nil
}

func (p *Parser) parseColumnDef() (ColumnDef, error) {
	name, err := p.parseIdent()
	if err != nil {
		return ColumnDef{}, err
	}
	if p.tok.Kind != Keyword {
		return ColumnDef{}, fmt.Errorf("sql: expected type for column %s", name)
	}
	var typ ColType
	switch p.tok.Keyword {
	case "INTEGER", "INT":
		typ = TypeInteger
	case "TEXT":
		typ = TypeText
	case "BLOB":
		typ = TypeBlob
	default:
		return ColumnDef{}, fmt.Errorf("sql: unsupported type %s", p.tok.Keyword)
	}
	if err := p.next(); err != nil {
		return ColumnDef{}, err
	}
	col := ColumnDef{Name: name, Type: typ}
	for p.tok.Kind == Keyword {
		switch p.tok.Keyword {
		case "PRIMARY":
			if err := p.next(); err != nil {
				return ColumnDef{}, err
			}
			if err := p.expectKeyword("KEY"); err != nil {
				return ColumnDef{}, err
			}
			col.PrimaryKey = true
			col.NotNull = true
		case "NOT":
			if err := p.next(); err != nil {
				return ColumnDef{}, err
			}
			if err := p.expectKeyword("NULL"); err != nil {
				return ColumnDef{}, err
			}
			col.NotNull = true
		case "NULL":
			if err := p.next(); err != nil {
				return ColumnDef{}, err
			}
		default:
			return col, nil
		}
	}
	return col, nil
}

func (p *Parser) parseDrop() (Stmt, error) {
	if err := p.next(); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	ifExists := false
	if p.tok.Kind == Keyword && p.tok.Keyword == "IF" {
		if err := p.next(); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifExists = true
	}
	name, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	return &DropTableStmt{Name: name, IfExists: ifExists}, nil
}

func (p *Parser) parseInsert() (Stmt, error) {
	if err := p.next(); err != nil {
		return nil, err
	}
	orReplace := false
	if p.tok.Kind == Keyword && p.tok.Keyword == "OR" {
		if err := p.next(); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("REPLACE"); err != nil {
			return nil, err
		}
		orReplace = true
	}
	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}
	table, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	var cols []string
	if p.tok.Kind == LParen {
		if err := p.next(); err != nil {
			return nil, err
		}
		for {
			c, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			cols = append(cols, c)
			if p.tok.Kind == Comma {
				if err := p.next(); err != nil {
					return nil, err
				}
				continue
			}
			break
		}
		if err := p.expect(RParen); err != nil {
			return nil, err
		}
	}
	if err := p.expectKeyword("VALUES"); err != nil {
		return nil, err
	}
	if err := p.expect(LParen); err != nil {
		return nil, err
	}
	var vals []Expr
	for {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		vals = append(vals, e)
		if p.tok.Kind == Comma {
			if err := p.next(); err != nil {
				return nil, err
			}
			continue
		}
		break
	}
	if err := p.expect(RParen); err != nil {
		return nil, err
	}
	return &InsertStmt{Table: table, OrReplace: orReplace, Columns: cols, Values: vals}, nil
}

func (p *Parser) parseUpdate() (Stmt, error) {
	if err := p.next(); err != nil {
		return nil, err
	}
	table, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}
	var sets []Assignment
	for {
		col, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		if err := p.expect(Eq); err != nil {
			return nil, err
		}
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		sets = append(sets, Assignment{Column: col, Value: e})
		if p.tok.Kind == Comma {
			if err := p.next(); err != nil {
				return nil, err
			}
			continue
		}
		break
	}
	where, err := p.parseWhereOptional()
	if err != nil {
		return nil, err
	}
	if where == nil {
		return nil, fmt.Errorf("sql: UPDATE requires WHERE pk = ?")
	}
	return &UpdateStmt{Table: table, Sets: sets, Where: where}, nil
}

func (p *Parser) parseDelete() (Stmt, error) {
	if err := p.next(); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	table, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	where, err := p.parseWhereOptional()
	if err != nil {
		return nil, err
	}
	if where == nil {
		return nil, fmt.Errorf("sql: DELETE requires WHERE pk = ?")
	}
	return &DeleteStmt{Table: table, Where: where}, nil
}

func (p *Parser) parseSelect() (Stmt, error) {
	if err := p.next(); err != nil {
		return nil, err
	}
	stmt := &SelectStmt{}
	if p.tok.Kind == Star {
		stmt.Star = true
		if err := p.next(); err != nil {
			return nil, err
		}
	} else {
		for {
			c, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			stmt.Columns = append(stmt.Columns, c)
			if p.tok.Kind == Comma {
				if err := p.next(); err != nil {
					return nil, err
				}
				continue
			}
			break
		}
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	table, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Table = table
	where, err := p.parseWhereOptional()
	if err != nil {
		return nil, err
	}
	stmt.Where = where
	return stmt, nil
}

func (p *Parser) parseWhereOptional() (*EqPred, error) {
	if p.tok.Kind != Keyword || p.tok.Keyword != "WHERE" {
		return nil, nil
	}
	if err := p.next(); err != nil {
		return nil, err
	}
	col, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expect(Eq); err != nil {
		return nil, err
	}
	e, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	return &EqPred{Column: col, Value: e}, nil
}

func (p *Parser) parseExpr() (Expr, error) {
	switch p.tok.Kind {
	case Param:
		p.params++
		idx := p.params
		if err := p.next(); err != nil {
			return nil, err
		}
		return ParamExpr{Index: idx}, nil
	case Int:
		v, err := strconv.ParseInt(p.tok.Lit, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("sql: bad integer %q", p.tok.Lit)
		}
		if err := p.next(); err != nil {
			return nil, err
		}
		return IntExpr{Value: v}, nil
	case String:
		s := p.tok.Lit
		if err := p.next(); err != nil {
			return nil, err
		}
		return TextExpr{Value: s}, nil
	case Blob:
		b, err := hex.DecodeString(p.tok.Lit)
		if err != nil {
			return nil, err
		}
		if err := p.next(); err != nil {
			return nil, err
		}
		return BlobExpr{Value: b}, nil
	case Keyword:
		if p.tok.Keyword == "NULL" {
			if err := p.next(); err != nil {
				return nil, err
			}
			return NullExpr{}, nil
		}
	}
	return nil, fmt.Errorf("sql: expected expression, got %q", p.tok.Lit)
}
