package parser

import (
	"fmt"
)

type ParseError struct {
	lastLiteral string
	goyaccErr   string
}

func wrapParseError(lit string, errStr string) error {
	return &ParseError{
		lastLiteral: lit,
		goyaccErr:   errStr,
	}
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parser error: %s, last token: %s", e.goyaccErr, e.lastLiteral)
}
