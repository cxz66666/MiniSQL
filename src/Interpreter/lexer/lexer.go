package lexer


import (
	"fmt"
	"io"
	"log"
)

var (
	UnexpectedTokenErr = fmt.Errorf("unexpected token")
)

type Tokenizer interface {
	FromStrLit(lit string,TokenType Token, lastToken int) int
}

type LexerImpl struct {
	scanner   *Scanner
	tokenizer Tokenizer
	Result    interface{}
}

type LexerResult struct {
	Token   int
	Literal string
}

func NewLexerImpl(r io.Reader, t Tokenizer) *LexerImpl {
	return &LexerImpl{
		scanner:   NewScanner(r),
		tokenizer: t,
	}
}

func (li *LexerImpl) Lex(lastToken int) (*LexerResult, error) {
	result := &LexerResult{}

SCAN:
	tok, lit := li.scanner.Scan()

	switch tok {
	case EOF:
		// Stop lex
	case IDENT, INTEGER,FLOAT,STRING, LEFT_PARENTHESIS, RIGHT_PARENTHESIS, COMMA, SEMICOLON, EQUAL, ANGLE_LEFT, ANGLE_RIGHT,ANGLE_LEFT_EQUAL,ANGLE_RIGHT_EQUAL,NOT_EQUAL,ASTERISK,POINT:
		result.Literal = lit
	case WS,APOSTROPNE:
		// Skip
		goto SCAN
	default:
		log.Printf("UnexpectedToken: tok is %d, lit is %s\n", tok, lit)
		return nil, UnexpectedTokenErr
	}

	result.Token = li.tokenizer.FromStrLit(lit,tok,lastToken)

	return result, nil
}
