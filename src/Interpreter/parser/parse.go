package parser

import (
	"io"
	"minisql/src/Interpreter/lexer"
	"minisql/src/Interpreter/types"
)

// Parse returns parsed Spanner DDL statements.
func Parse(r io.Reader,channel chan<- types.DStatements) error {
	impl := lexer.NewLexerImpl(r, &keywordTokenizer{})
	l := newLexerWrapper(impl,channel)
	yyParse(l)
	if l.err != nil {
		return  l.err
	}
	return nil
}
