package parser

import (
	"minisql/src/Interpreter/lexer"
	"testing"
)

func TestFromStrLit(t *testing.T) {
	cases := []struct {
		lit       string
		TokenType lexer.Token
		lastToken int
		expect    int
	}{
		{
			"10",
			lexer.INTEGER,
			0,
			decimal_value,
		},
		{
			"0xff",
			lexer.INTEGER,
			0,
			hex_value,
		},
		// Not hex value
		{
			"ff",
			lexer.INTEGER,
			0,
			0,
		},
	}

	tk := keywordTokenizer{}
	for _, c := range cases {
		actual := tk.FromStrLit(c.lit,c.TokenType, c.lastToken)
		if actual != c.expect {
			t.Errorf("Expected: %v, but actual: %v\n", c.expect, actual)
		}
	}
}
