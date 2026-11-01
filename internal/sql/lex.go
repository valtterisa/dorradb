// package sql tokenizes a small SQL dialect for DorraDB.
package sql

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Kind int

const (
	EOF Kind = iota
	Ident
	String // single-quoted text
	Blob   // X'...'
	Int
	Param // ?

	Keyword // see Keyword value

	Comma
	Semi
	LParen
	RParen
	Star
	Eq
)

type Token struct {
	Kind    Kind
	Lit     string // keyword uppercased; ident as written; literal text
	Keyword string // when Kind==Keyword
	Pos     int
}

type Lexer struct {
	src  string
	pos  int
	wpos int // start of current token
}

func newLexer(src string) *Lexer { return &Lexer{src: src} }

var keywords = map[string]bool{
	"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true,
	"CREATE": true, "TABLE": true, "DROP": true, "FROM": true, "WHERE": true,
	"SET": true, "VALUES": true, "INTO": true, "OR": true, "REPLACE": true,
	"IF": true, "NOT": true, "EXISTS": true, "PRIMARY": true, "KEY": true,
	"NULL": true, "AND": true, "AS": true,
	"INTEGER": true, "INT": true, "TEXT": true, "BLOB": true,
}

func (l *Lexer) Next() (Token, error) {
	l.skipSpace()
	if l.pos >= len(l.src) {
		return Token{Kind: EOF, Pos: l.pos}, nil
	}
	l.wpos = l.pos
	ch := l.src[l.pos]

	switch ch {
	case ',':
		l.pos++
		return Token{Kind: Comma, Lit: ",", Pos: l.wpos}, nil
	case ';':
		l.pos++
		return Token{Kind: Semi, Lit: ";", Pos: l.wpos}, nil
	case '(':
		l.pos++
		return Token{Kind: LParen, Lit: "(", Pos: l.wpos}, nil
	case ')':
		l.pos++
		return Token{Kind: RParen, Lit: ")", Pos: l.wpos}, nil
	case '*':
		l.pos++
		return Token{Kind: Star, Lit: "*", Pos: l.wpos}, nil
	case '=':
		l.pos++
		return Token{Kind: Eq, Lit: "=", Pos: l.wpos}, nil
	case '?':
		l.pos++
		return Token{Kind: Param, Lit: "?", Pos: l.wpos}, nil
	case '\'':
		return l.scanString()
	}

	if ch == 'X' || ch == 'x' {
		if l.pos+1 < len(l.src) && l.src[l.pos+1] == '\'' {
			return l.scanBlob()
		}
	}

	if ch == '-' || ch == '+' || unicode.IsDigit(rune(ch)) {
		return l.scanNumber()
	}

	r, _ := utf8.DecodeRuneInString(l.src[l.pos:])
	if isIdentStart(r) {
		return l.scanIdent()
	}

	return Token{}, fmt.Errorf("sql: unexpected character %q at %d", ch, l.pos)
}

func (l *Lexer) skipSpace() {
	for l.pos < len(l.src) {
		ch := l.src[l.pos]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			l.pos++
			continue
		}
		// -- line comment
		if ch == '-' && l.pos+1 < len(l.src) && l.src[l.pos+1] == '-' {
			l.pos += 2
			for l.pos < len(l.src) && l.src[l.pos] != '\n' {
				l.pos++
			}
			continue
		}
		break
	}
}

func (l *Lexer) scanIdent() (Token, error) {
	for l.pos < len(l.src) {
		r, size := utf8.DecodeRuneInString(l.src[l.pos:])
		if !isIdentCont(r) {
			break
		}
		l.pos += size
	}
	lit := l.src[l.wpos:l.pos]
	upper := strings.ToUpper(lit)
	if keywords[upper] {
		return Token{Kind: Keyword, Lit: upper, Keyword: upper, Pos: l.wpos}, nil
	}
	return Token{Kind: Ident, Lit: lit, Pos: l.wpos}, nil
}

func (l *Lexer) scanNumber() (Token, error) {
	if l.src[l.pos] == '+' || l.src[l.pos] == '-' {
		l.pos++
	}
	startDigits := l.pos
	for l.pos < len(l.src) && l.src[l.pos] >= '0' && l.src[l.pos] <= '9' {
		l.pos++
	}
	if l.pos == startDigits {
		return Token{}, fmt.Errorf("sql: bad number at %d", l.wpos)
	}
	return Token{Kind: Int, Lit: l.src[l.wpos:l.pos], Pos: l.wpos}, nil
}

func (l *Lexer) scanString() (Token, error) {
	l.pos++ // opening '
	var b strings.Builder
	for l.pos < len(l.src) {
		ch := l.src[l.pos]
		if ch == '\'' {
			if l.pos+1 < len(l.src) && l.src[l.pos+1] == '\'' {
				b.WriteByte('\'')
				l.pos += 2
				continue
			}
			l.pos++
			return Token{Kind: String, Lit: b.String(), Pos: l.wpos}, nil
		}
		b.WriteByte(ch)
		l.pos++
	}
	return Token{}, fmt.Errorf("sql: unterminated string at %d", l.wpos)
}

func (l *Lexer) scanBlob() (Token, error) {
	l.pos += 2 // X'
	start := l.pos
	for l.pos < len(l.src) && l.src[l.pos] != '\'' {
		l.pos++
	}
	if l.pos >= len(l.src) {
		return Token{}, fmt.Errorf("sql: unterminated blob at %d", l.wpos)
	}
	hex := l.src[start:l.pos]
	l.pos++ // closing '
	if len(hex)%2 != 0 {
		return Token{}, fmt.Errorf("sql: odd-length blob literal at %d", l.wpos)
	}
	for _, c := range hex {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return Token{}, fmt.Errorf("sql: bad blob hex at %d", l.wpos)
		}
	}
	return Token{Kind: Blob, Lit: strings.ToUpper(hex), Pos: l.wpos}, nil
}

func isIdentStart(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

func isIdentCont(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
