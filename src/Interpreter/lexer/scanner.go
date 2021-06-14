//scanning from the Scanner struct and return the Token type

package lexer

import (
	"bufio"
	"bytes"
	"io"
)

// Token represents a lexical token.
type Token int

const (
	// Special tokens
	ILLEGAL Token = iota
	EOF
	WS
	APOSTROPNE
	// Literals
	IDENT  // main   almost all string , not 'string'(don't conatin \'\')
	INTEGER // number literal
	FLOAT
	STRING
	// Misc characters
	ASTERISK          // *
	COMMA             // ,
	LEFT_PARENTHESIS  // (
	RIGHT_PARENTHESIS // )
	SEMICOLON         // ;
	EQUAL             // =
	ANGLE_LEFT        // <
	ANGLE_LEFT_EQUAL  //<=
	ANGLE_RIGHT_EQUAL //>=
	ANGLE_RIGHT       // >
	NOT_EQUAL 		  // <> or !=
	POINT            //  .
)

// Scanner represents a lexical scanner.
type Scanner struct {
	r *bufio.Reader
	apostropne bool // apostropne is true means
}

// NewScanner returns a new instance of Scanner.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: bufio.NewReader(r),apostropne: false}
}

// Scan returns the next token and literal value.
func (s *Scanner) Scan() (tok Token, lit string) {
	// Read the next rune.
	ch := s.read()

	// If we see whitespace then consume all contiguous whitespace.
	// If we see a letter then consume as an ident or reserved word.
	// If we see a digit then consume as a number.
	if isWhitespace(ch) {
		s.unread()
		return s.scanWhitespace()
	} else if isLetter(ch) {
		s.unread()
		return s.scanIdent()
	} else if isDigit(ch) {
		s.unread()
		return s.scanNumber()
	}

	// Otherwise read the individual character.
	switch ch {
	case eof:
		return EOF, ""
	case '\'':
		s.apostropne=!s.apostropne
		if s.apostropne {
			return s.scanString()
		}
		return APOSTROPNE,string(ch)
	case '.':
		return POINT,string(ch)
	case '*':
		return ASTERISK, string(ch)
	case ',':
		return COMMA, string(ch)
	case '(':
		return LEFT_PARENTHESIS, string(ch)
	case ')':
		return RIGHT_PARENTHESIS, string(ch)
	case ';':
		return SEMICOLON, string(ch)
	case '=':
		return EQUAL, string(ch)
	case '<':
		ch_next:=s.read()
		if string(ch_next)=="=" {
			return ANGLE_LEFT_EQUAL,"<="
		} else if string(ch_next)==">"{
			return NOT_EQUAL, "<>"
		}
		s.unread()
		// in fact it's no use for the above code
		return ANGLE_LEFT, string(ch)
	case '>':
		ch_next:=s.read()
		if string(ch_next)=="=" {
			return ANGLE_RIGHT_EQUAL,">="
		}
		s.unread()
		//the same as this one
		return ANGLE_RIGHT, string(ch)
	case '!':
		ch_next:=s.read()
		if string(ch_next)=="=" {
			return NOT_EQUAL,"<>"
		}
		s.unread()
	}

	return ILLEGAL, string(ch)
}

// scanWhitespace consumes the current rune and all contiguous whitespace.
func (s *Scanner) scanWhitespace() (tok Token, lit string) {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read every subsequent whitespace character into the buffer.
	// Non-whitespace characters and EOF will cause the loop to exit.
	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isWhitespace(ch) {
			s.unread()
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	return WS, buf.String()
}

// scanIdent consumes the current rune and all contiguous ident runes.
func (s *Scanner) scanIdent() (tok Token, lit string) {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read every subsequent ident character into the buffer.
	// Non-ident characters and EOF will cause the loop to exit.
	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isLetter(ch) && !isDigit(ch) && ch != '_' {
			s.unread()
			break
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}
	if s.apostropne {
		return STRING,buf.String()
	}
	return IDENT, buf.String()
}

// scanIdent consumes the current rune and all contiguous ident runes.
// NOTE: 0b and 0x prefixes are not supported.
func (s *Scanner) scanNumber() (tok Token, lit string) {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read every subsequent ident character into the buffer.
	// Non-ident characters and EOF will cause the loop to exit.
	for {
		if ch := s.read(); ch == eof {
			return INTEGER, buf.String()
		} else if !isDigit(ch) {
			if ch!='.' {
				s.unread()
				return INTEGER,buf.String()
			} //include point
			_, _ = buf.WriteRune(ch)
			for {
				if ch := s.read(); ch == eof{
					return FLOAT, buf.String()
				} else if !isDigit(ch){
					s.unread()
					return FLOAT,buf.String()
				} else {
					_ , _=buf.WriteRune(ch)
				}
			}
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}

	return INTEGER, buf.String()
}


//scanString will read the 'cxz' like these strings
func (s *Scanner) scanString() (tok Token, lit string) {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	for {
		if ch := s.read(); ch == eof {
			return ILLEGAL, buf.String()
		} else if ch=='\'' {
			s.unread()
			return STRING,buf.String()
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}
	return ILLEGAL,buf.String()
}
// read reads the next rune from the buffered reader.
// Returns the rune(0) if an error occurs (or io.EOF is returned).
func (s *Scanner) read() rune {
	ch, _, err := s.r.ReadRune()
	if err != nil {
		return eof
	}
	return ch
}

// unread places the previously read rune back on the reader.
func (s *Scanner) unread() { _ = s.r.UnreadRune() }

// isWhitespace returns true if the rune is a space, tab, or newline.
func isWhitespace(ch rune) bool { return ch == ' ' || ch == '\t' || ch == '\n' || ch== '\r' }

// isLetter returns true if the rune is a letter.
func isLetter(ch rune) bool { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') }

// isDigit returns true if the rune is a digit.
func isDigit(ch rune) bool { return (ch >= '0' && ch <= '9') }

// eof represents a marker rune for the end of the reader.
var eof = rune(0)
