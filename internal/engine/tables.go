package engine

import (
	"encoding/binary"
	"strings"

	"dorradb/internal/btree"
	"dorradb/internal/sql"
)

// WAL key routing for named tables (encode.Record layout unchanged).
// Layout: 0xFF 0x00 | u16 tableLen | tableName | userKey
const (
	routeMark0 = 0xFF
	routeMark1 = 0x00
)

func encodeRoutedKey(table string, key []byte) []byte {
	if table == "" {
		return key
	}
	out := make([]byte, 0, 4+len(table)+len(key))
	out = append(out, routeMark0, routeMark1)
	out = binary.LittleEndian.AppendUint16(out, uint16(len(table)))
	out = append(out, table...)
	out = append(out, key...)
	return out
}

func decodeRoutedKey(k []byte) (table string, key []byte) {
	if len(k) < 4 || k[0] != routeMark0 || k[1] != routeMark1 {
		return "", k
	}
	n := int(binary.LittleEndian.Uint16(k[2:4]))
	if n < 0 || 4+n > len(k) {
		return "", k
	}
	return string(k[4 : 4+n]), k[4+n:]
}

func (e *Engine) tableTree(name string) (*btree.Tree, error) {
	if name == "" {
		return e.tree, nil
	}
	t, ok := e.tables[strings.ToLower(name)]
	if !ok {
		return nil, ErrTableNotFound
	}
	return t, nil
}

func (e *Engine) rootPersister(name string) func(uint32) error {
	lname := strings.ToLower(name)
	return func(root uint32) error {
		tab, ok := e.cat.Get(lname)
		if !ok {
			return nil
		}
		tab.Root = root
		e.catalogDirty = true
		return nil
	}
}

func (e *Engine) loadCatalog() error {
	if e.cat == nil {
		e.cat = sql.NewCatalog()
	}
	if e.tables == nil {
		e.tables = make(map[string]*btree.Tree)
	}
	// If replay already populated the catalog, just attach any missing trees.
	if len(e.cat.Tables) > 0 {
		for _, tab := range e.cat.Tables {
			lname := strings.ToLower(tab.Name)
			if _, ok := e.tables[lname]; ok {
				continue
			}
			tr := btree.Attach(e.main, tab.Root, e.rootPersister(tab.Name))
			tr.SetFlusher(e.flushPages)
			e.tables[lname] = tr
		}
		return nil
	}
	val, ok, err := e.tree.GetVisible([]byte(sql.SystemKey), e.appliedSequence)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	c, err := sql.Decode(val)
	if err != nil {
		return err
	}
	e.cat = c
	for _, tab := range c.Tables {
		lname := strings.ToLower(tab.Name)
		tr := btree.Attach(e.main, tab.Root, e.rootPersister(tab.Name))
		tr.SetFlusher(e.flushPages)
		e.tables[lname] = tr
	}
	return nil
}

// Catalog returns table metadata (engine-owned; treat as read-only).
func (e *Engine) Catalog() *sql.Catalog {
	return e.cat
}
