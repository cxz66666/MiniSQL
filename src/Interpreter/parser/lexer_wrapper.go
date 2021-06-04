package parser

import (
	"log"
	"minisql/src/Interpreter/lexer"
	"minisql/src/Interpreter/types"
)

type lexerWrapper struct {
	impl        *lexer.LexerImpl
	result      []types.DStatements
	lastLiteral string
	err         error
}

func newLexerWrapper(li *lexer.LexerImpl) *lexerWrapper {
	return &lexerWrapper{
		impl: li,
	}
}

func (l *lexerWrapper) Lex(lval *yySymType) int {
	r, err := l.impl.Lex(lval.LastToken)
	if err != nil {
		log.Fatal(err)
	}
	l.lastLiteral = r.Literal

	tokVal := r.Token
	lval.str = r.Literal
	lval.LastToken = tokVal

	return tokVal
}

func (l *lexerWrapper) Error(errStr string) {
	l.err = wrapParseError(l.lastLiteral, errStr)
}
