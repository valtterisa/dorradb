// package sql encodes/decodes NSM row records stored in B+ leaf cell values.
package sql

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var (
	ErrNull    = errors.New("row: NULL not allowed")
	ErrType    = errors.New("row: type mismatch")
	ErrCorrupt = errors.New("row: corrupt record")
)

const (
	tagNull uint8 = 0
	tagInt  uint8 = 1
	tagText uint8 = 2
	tagBlob uint8 = 3
)

// Value is a typed SQL value.
type Value struct {
	Null  bool
	Int   int64
	Text  string
	Blob  []byte
	Type  ColType // meaningful when !Null
}

func IntValue(v int64) Value   { return Value{Int: v, Type: TypeInteger} }
func TextValue(s string) Value { return Value{Text: s, Type: TypeText} }
func BlobValue(b []byte) Value {
	return Value{Blob: append([]byte(nil), b...), Type: TypeBlob}
}
func NullValue() Value { return Value{Null: true} }

// EncodePK encodes a primary-key value as a tree key.
func EncodePK(col Column, v Value) ([]byte, error) {
	if v.Null {
		return nil, ErrNull
	}
	switch col.Type {
	case TypeInteger:
		if v.Type != TypeInteger {
			return nil, ErrType
		}
		buf := make([]byte, 8)
		// big-endian so byte order matches numeric order for non-negative;
		// XOR sign bit for full int64 order.
		u := uint64(v.Int) ^ (1 << 63)
		binary.BigEndian.PutUint64(buf, u)
		return buf, nil
	case TypeText:
		if v.Type != TypeText {
			return nil, ErrType
		}
		return []byte(v.Text), nil
	case TypeBlob:
		if v.Type != TypeBlob {
			return nil, ErrType
		}
		return append([]byte(nil), v.Blob...), nil
	default:
		return nil, ErrType
	}
}

// DecodePK decodes a tree key into a PK value.
func DecodePK(col Column, key []byte) (Value, error) {
	switch col.Type {
	case TypeInteger:
		if len(key) != 8 {
			return Value{}, ErrCorrupt
		}
		u := binary.BigEndian.Uint64(key) ^ (1 << 63)
		return IntValue(int64(u)), nil
	case TypeText:
		return TextValue(string(key)), nil
	case TypeBlob:
		return BlobValue(key), nil
	default:
		return Value{}, ErrType
	}
}

// EncodeRecord encodes non-PK columns in table order into a version payload.
func EncodeRecord(cols []Column, values []Value) ([]byte, error) {
	if len(cols) != len(values) {
		return nil, fmt.Errorf("row: column count mismatch")
	}
	buf := make([]byte, 0, 64)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(cols)))
	for i, col := range cols {
		v := values[i]
		if v.Null {
			if col.NotNull {
				return nil, ErrNull
			}
			buf = append(buf, tagNull)
			continue
		}
		switch col.Type {
		case TypeInteger:
			if v.Type != TypeInteger {
				return nil, ErrType
			}
			buf = append(buf, tagInt)
			buf = binary.LittleEndian.AppendUint64(buf, uint64(v.Int))
		case TypeText:
			if v.Type != TypeText {
				return nil, ErrType
			}
			b := []byte(v.Text)
			buf = append(buf, tagText)
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(b)))
			buf = append(buf, b...)
		case TypeBlob:
			if v.Type != TypeBlob {
				return nil, ErrType
			}
			buf = append(buf, tagBlob)
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(v.Blob)))
			buf = append(buf, v.Blob...)
		default:
			return nil, ErrType
		}
	}
	return buf, nil
}

// DecodeRecord decodes a version payload into non-PK values (table column order).
func DecodeRecord(cols []Column, buf []byte) ([]Value, error) {
	if len(buf) < 2 {
		return nil, ErrCorrupt
	}
	n := int(binary.LittleEndian.Uint16(buf[0:2]))
	if n != len(cols) {
		return nil, ErrCorrupt
	}
	off := 2
	out := make([]Value, n)
	for i := 0; i < n; i++ {
		if off >= len(buf) {
			return nil, ErrCorrupt
		}
		tag := buf[off]
		off++
		switch tag {
		case tagNull:
			out[i] = NullValue()
		case tagInt:
			if off+8 > len(buf) {
				return nil, ErrCorrupt
			}
			out[i] = IntValue(int64(binary.LittleEndian.Uint64(buf[off : off+8])))
			off += 8
		case tagText:
			if off+4 > len(buf) {
				return nil, ErrCorrupt
			}
			l := int(binary.LittleEndian.Uint32(buf[off : off+4]))
			off += 4
			if off+l > len(buf) {
				return nil, ErrCorrupt
			}
			out[i] = TextValue(string(buf[off : off+l]))
			off += l
		case tagBlob:
			if off+4 > len(buf) {
				return nil, ErrCorrupt
			}
			l := int(binary.LittleEndian.Uint32(buf[off : off+4]))
			off += 4
			if off+l > len(buf) {
				return nil, ErrCorrupt
			}
			out[i] = BlobValue(buf[off : off+l])
			off += l
		default:
			return nil, ErrCorrupt
		}
	}
	return out, nil
}

// Project builds a full row (PK + non-PK) as []Value in table column order.
func Project(t *Table, pkKey []byte, record []byte) ([]Value, error) {
	pk := t.PK()
	pkVal, err := DecodePK(pk, pkKey)
	if err != nil {
		return nil, err
	}
	non := t.NonPK()
	vals, err := DecodeRecord(non, record)
	if err != nil {
		return nil, err
	}
	out := make([]Value, len(t.Columns))
	vi := 0
	for i, c := range t.Columns {
		if c.PrimaryKey {
			out[i] = pkVal
		} else {
			out[i] = vals[vi]
			vi++
		}
	}
	return out, nil
}
