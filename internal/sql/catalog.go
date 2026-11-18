// package sql stores table schemas and B+ tree root page ids.
package sql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

const (
	magic   uint32 = 0x54435244 // "DRCT"
	version uint8  = 1

	// SystemKey is the catalog blob key on the bootstrap tree.
	SystemKey = "\x00catalog"
)

var ErrExists = errors.New("catalog: table exists")

type Column struct {
	Name       string
	Type       ColType
	PrimaryKey bool
	NotNull    bool
}

type Table struct {
	Name    string
	Root    uint32
	Columns []Column
}

func (t *Table) PK() Column {
	for _, c := range t.Columns {
		if c.PrimaryKey {
			return c
		}
	}
	return Column{}
}

func (t *Table) NonPK() []Column {
	out := make([]Column, 0, len(t.Columns)-1)
	for _, c := range t.Columns {
		if !c.PrimaryKey {
			out = append(out, c)
		}
	}
	return out
}

func (t *Table) Col(name string) (Column, int, bool) {
	for i, c := range t.Columns {
		if strings.EqualFold(c.Name, name) {
			return c, i, true
		}
	}
	return Column{}, -1, false
}

// Catalog is the in-memory table map.
type Catalog struct {
	Tables map[string]*Table // lower-case name → table
}

func NewCatalog() *Catalog {
	return &Catalog{Tables: make(map[string]*Table)}
}

func (c *Catalog) Get(name string) (*Table, bool) {
	t, ok := c.Tables[strings.ToLower(name)]
	return t, ok
}

func (c *Catalog) Put(t *Table) error {
	key := strings.ToLower(t.Name)
	if _, ok := c.Tables[key]; ok {
		return ErrExists
	}
	c.Tables[key] = t
	return nil
}

func (c *Catalog) Remove(name string) {
	delete(c.Tables, strings.ToLower(name))
}

func (c *Catalog) Encode() []byte {
	n := 4 + 1 + 2
	for _, t := range c.Tables {
		n += 2 + len(t.Name) + 4 + 2
		for _, col := range t.Columns {
			n += 2 + len(col.Name) + 1 + 1
		}
	}
	buf := make([]byte, 0, n)
	buf = binary.LittleEndian.AppendUint32(buf, magic)
	buf = append(buf, version)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(c.Tables)))
	for _, t := range c.Tables {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(t.Name)))
		buf = append(buf, t.Name...)
		buf = binary.LittleEndian.AppendUint32(buf, t.Root)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(t.Columns)))
		for _, col := range t.Columns {
			buf = binary.LittleEndian.AppendUint16(buf, uint16(len(col.Name)))
			buf = append(buf, col.Name...)
			buf = append(buf, byte(col.Type))
			var flags byte
			if col.PrimaryKey {
				flags |= 1
			}
			if col.NotNull {
				flags |= 2
			}
			buf = append(buf, flags)
		}
	}
	return buf
}

func Decode(buf []byte) (*Catalog, error) {
	if len(buf) < 7 {
		return nil, errors.New("catalog: short")
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != magic {
		return nil, errors.New("catalog: bad magic")
	}
	if buf[4] != version {
		return nil, fmt.Errorf("catalog: unsupported version %d", buf[4])
	}
	n := int(binary.LittleEndian.Uint16(buf[5:7]))
	off := 7
	c := NewCatalog()
	for i := 0; i < n; i++ {
		if off+2 > len(buf) {
			return nil, errors.New("catalog: short name")
		}
		nl := int(binary.LittleEndian.Uint16(buf[off : off+2]))
		off += 2
		if off+nl+4+2 > len(buf) {
			return nil, errors.New("catalog: short table")
		}
		name := string(buf[off : off+nl])
		off += nl
		root := binary.LittleEndian.Uint32(buf[off : off+4])
		off += 4
		nc := int(binary.LittleEndian.Uint16(buf[off : off+2]))
		off += 2
		cols := make([]Column, 0, nc)
		for j := 0; j < nc; j++ {
			if off+2 > len(buf) {
				return nil, errors.New("catalog: short col")
			}
			cl := int(binary.LittleEndian.Uint16(buf[off : off+2]))
			off += 2
			if off+cl+2 > len(buf) {
				return nil, errors.New("catalog: short col body")
			}
			cname := string(buf[off : off+cl])
			off += cl
			typ := ColType(buf[off])
			flags := buf[off+1]
			off += 2
			cols = append(cols, Column{
				Name:       cname,
				Type:       typ,
				PrimaryKey: flags&1 != 0,
				NotNull:    flags&2 != 0,
			})
		}
		t := &Table{Name: name, Root: root, Columns: cols}
		c.Tables[strings.ToLower(name)] = t
	}
	return c, nil
}
