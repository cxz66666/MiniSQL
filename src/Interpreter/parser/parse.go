package parser

import (
	"io"
	"minisql/src/Interpreter/lexer"
	"minisql/src/Interpreter/types"
)

// Parse returns parsed Spanner DDL statements.
func Parse(r io.Reader) (*[]types.DStatements, error) {
	impl := lexer.NewLexerImpl(r, &keywordTokenizer{})
	l := newLexerWrapper(impl)
	yyParse(l)
	if l.err != nil {
		return nil, l.err
	} else {
		return &l.result, nil
	}
}
