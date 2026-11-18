// package sql holds SQL statement nodes for DorraDB's dialect.
package sql

type Stmt interface {
	stmt()
}

type ColType int

const (
	TypeInteger ColType = iota + 1
	TypeText
	TypeBlob
)

type ColumnDef struct {
	Name       string
	Type       ColType
	PrimaryKey bool
	NotNull    bool
}

type CreateTableStmt struct {
	Name        string
	IfNotExists bool
	Columns     []ColumnDef
}

func (CreateTableStmt) stmt() {}

type DropTableStmt struct {
	Name        string
	IfExists    bool
}

func (DropTableStmt) stmt() {}

type InsertStmt struct {
	Table     string
	OrReplace bool
	Columns   []string // empty = all
	Values    []Expr   // one row
}

func (InsertStmt) stmt() {}

type UpdateStmt struct {
	Table  string
	Sets   []Assignment
	Where  *EqPred
}

func (UpdateStmt) stmt() {}

type Assignment struct {
	Column string
	Value  Expr
}

type DeleteStmt struct {
	Table string
	Where *EqPred
}

func (DeleteStmt) stmt() {}

type SelectStmt struct {
	Star    bool
	Columns []string // when !Star
	Table   string
	Where   *EqPred
}

func (SelectStmt) stmt() {}

// EqPred is column = expr (only equality supported).
type EqPred struct {
	Column string
	Value  Expr
}

type Expr interface {
	expr()
}

type ParamExpr struct{ Index int } // 1-based
func (ParamExpr) expr() {}

type IntExpr struct{ Value int64 }
func (IntExpr) expr() {}

type TextExpr struct{ Value string }
func (TextExpr) expr() {}

type BlobExpr struct{ Value []byte }
func (BlobExpr) expr() {}

type NullExpr struct{}
func (NullExpr) expr() {}
