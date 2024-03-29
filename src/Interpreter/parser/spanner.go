// Code generated by goyacc -o src/Interpreter/parser/spanner.go src/Interpreter/parser/spanner.go.y. DO NOT EDIT.

//line src/Interpreter/parser/spanner.go.y:1

package parser

import __yyfmt__ "fmt"

//line src/Interpreter/parser/spanner.go.y:3

import (
	"minisql/src/Interpreter/types"
	Value "minisql/src/Interpreter/value"
	"strconv"
)

//line src/Interpreter/parser/spanner.go.y:11
type yySymType struct {
	yys           int
	empty         struct{}
	flag          bool
	i64           int64
	int           int
	f64           float64
	str           string
	strs          []string
	col           types.Column
	cols          []types.Column
	coltype       types.ColumnType
	key           types.Key
	keys          []types.Key
	keyorder      types.KeyOrder
	clstr         types.Cluster
	ondelete      types.OnDelete
	stcls         types.StoringClause
	intlr         types.Interleave
	intlrs        []types.Interleave
	fieldsname    types.FieldsName
	LastToken     int
	expr          types.Expr
	where         *types.Where
	limit         types.Limit
	compare       Value.CompareType
	valuetype     Value.Value
	valuetypelist []Value.Value
	setexpr       types.SetExpr
	setexprlist   []types.SetExpr
}

const IDENT = 57346
const IDENT_LEGAL = 57347
const PRIMARY = 57348
const KEY = 57349
const ASC = 57350
const DESC = 57351
const IN = 57352
const INTERLEAVE = 57353
const AND = 57354
const OR = 57355
const NOT = 57356
const NULL = 57357
const ON = 57358
const CASCADE = 57359
const NO = 57360
const ACTION = 57361
const MAX = 57362
const UNIQUE = 57363
const ADD = 57364
const COLUMN = 57365
const SET = 57366
const TRUE = 57367
const FALSE = 57368
const allow_commit_timestamp = 57369
const LE = 57370
const GE = 57371
const NE = 57372
const CREATE = 57373
const DROP = 57374
const EXECFILE = 57375
const USE = 57376
const DATABASE = 57377
const TABLE = 57378
const INDEX = 57379
const STORING = 57380
const SELECT = 57381
const WHERE = 57382
const FROM = 57383
const LIMIT = 57384
const OFFSET = 57385
const VALUES = 57386
const INSERT = 57387
const INTO = 57388
const UPDATE = 57389
const DELETE = 57390
const BOOL = 57391
const INT64 = 57392
const FLOAT64 = 57393
const STRING = 57394
const BYTES = 57395
const DATE = 57396
const TIMESTAMP = 57397
const database_id = 57398
const table_name = 57399
const column_name = 57400
const index_name = 57401
const decimal_value = 57402
const hex_value = 57403
const float_value = 57404
const string_value = 57405

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"IDENT",
	"IDENT_LEGAL",
	"PRIMARY",
	"KEY",
	"ASC",
	"DESC",
	"IN",
	"INTERLEAVE",
	"AND",
	"OR",
	"NOT",
	"NULL",
	"ON",
	"CASCADE",
	"NO",
	"ACTION",
	"MAX",
	"UNIQUE",
	"ADD",
	"COLUMN",
	"SET",
	"TRUE",
	"FALSE",
	"allow_commit_timestamp",
	"'('",
	"','",
	"')'",
	"';'",
	"'*'",
	"'.'",
	"'='",
	"'<'",
	"'>'",
	"LE",
	"GE",
	"NE",
	"CREATE",
	"DROP",
	"EXECFILE",
	"USE",
	"DATABASE",
	"TABLE",
	"INDEX",
	"STORING",
	"SELECT",
	"WHERE",
	"FROM",
	"LIMIT",
	"OFFSET",
	"VALUES",
	"INSERT",
	"INTO",
	"UPDATE",
	"DELETE",
	"BOOL",
	"INT64",
	"FLOAT64",
	"STRING",
	"BYTES",
	"DATE",
	"TIMESTAMP",
	"database_id",
	"table_name",
	"column_name",
	"index_name",
	"decimal_value",
	"hex_value",
	"float_value",
	"string_value",
}

var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyInitialStackSize = 16

//line yacctab:1
var yyExca = [...]int{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyPrivate = 57344

const yyLast = 243

var yyAct = [...]int{
	96, 46, 201, 171, 97, 170, 122, 142, 94, 38,
	83, 130, 77, 147, 53, 54, 104, 169, 53, 54,
	79, 148, 149, 51, 98, 104, 102, 103, 69, 104,
	61, 53, 54, 118, 86, 102, 103, 64, 95, 102,
	103, 112, 113, 114, 60, 115, 116, 117, 56, 179,
	49, 66, 59, 58, 55, 48, 152, 120, 92, 62,
	196, 50, 75, 72, 80, 173, 148, 149, 81, 57,
	105, 106, 107, 99, 40, 88, 93, 90, 80, 105,
	106, 107, 99, 105, 106, 107, 99, 74, 174, 121,
	67, 47, 87, 35, 123, 80, 34, 91, 126, 52,
	15, 17, 22, 16, 129, 125, 39, 138, 18, 137,
	41, 42, 43, 33, 19, 32, 20, 21, 45, 140,
	144, 131, 132, 133, 134, 135, 136, 127, 128, 36,
	37, 157, 63, 209, 181, 156, 153, 154, 158, 182,
	198, 160, 159, 151, 197, 155, 182, 183, 151, 150,
	63, 124, 108, 109, 204, 31, 175, 30, 29, 168,
	28, 27, 26, 25, 24, 143, 63, 203, 177, 178,
	176, 146, 145, 89, 68, 65, 39, 199, 191, 192,
	164, 123, 85, 189, 190, 70, 193, 187, 188, 180,
	127, 128, 202, 166, 205, 185, 186, 161, 84, 141,
	53, 54, 47, 172, 78, 206, 208, 207, 84, 73,
	2, 14, 23, 13, 12, 11, 10, 9, 8, 7,
	6, 5, 4, 3, 1, 76, 119, 44, 200, 194,
	195, 71, 165, 163, 162, 139, 184, 101, 100, 167,
	111, 110, 82,
}

var yyPact = [...]int{
	60, 60, -1000, 133, 132, 131, 130, 129, 127, 126,
	124, 84, 82, 65, 62, 85, 30, 66, 86, 0,
	-16, 11, 27, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -11, -18, 23, -1000,
	-12, -13, -22, -38, 9, -1000, 137, -1000, -29, 151,
	196, 57, -1000, -1000, -1000, -1000, 146, -40, -1000, -1000,
	-1000, 169, 196, 204, 34, 199, 15, 196, 203, 166,
	-32, 46, -1000, -1000, 145, 197, 29, -1000, 42, -1000,
	10, -1000, 123, -1000, -17, -33, -1000, 6, 196, 1,
	121, -1000, 199, 1, 178, 10, 87, 87, 10, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 193, 136,
	155, -1000, -1000, -1000, -1000, 144, -1000, -1000, 143, -1000,
	-48, -1000, 119, -1000, 3, -1000, -1000, 10, 10, 115,
	14, -1000, -1000, -1000, -1000, -1000, -1000, 14, 178, 111,
	-1000, 190, -1000, 164, 179, -3, 198, 36, -1000, -1000,
	-1000, 1, 142, 178, 178, -1000, -1000, -1000, -1000, -1000,
	136, 141, -1000, -1000, -8, -1000, 174, 104, -1000, -1000,
	117, -1000, 187, -48, -48, -1000, 1, -1000, 198, 161,
	-1000, -1000, 198, 13, -1000, -1000, -1000, -1000, -1000, 114,
	110, -1000, 158, -1000, 181, -1000, 139, -1000, -1000, -1000,
	125, -1000, 184, 197, 181, 196, 103, -1000, -1000, -1000,
}

var yyPgo = [...]int{
	0, 10, 242, 241, 240, 239, 238, 237, 236, 3,
	5, 235, 7, 234, 233, 232, 9, 0, 1, 231,
	230, 229, 2, 228, 227, 8, 226, 11, 4, 13,
	20, 12, 225, 6, 224, 210, 223, 222, 221, 220,
	219, 218, 217, 216, 215, 214, 213, 211,
}

var yyR1 = [...]int{
	0, 34, 34, 35, 35, 35, 35, 35, 35, 35,
	35, 35, 35, 35, 35, 47, 47, 47, 36, 37,
	38, 38, 2, 2, 2, 1, 11, 11, 10, 10,
	9, 8, 8, 8, 12, 12, 13, 14, 14, 14,
	3, 4, 4, 4, 4, 4, 4, 5, 5, 15,
	15, 39, 16, 16, 21, 21, 20, 18, 18, 23,
	23, 23, 22, 40, 41, 42, 44, 44, 45, 32,
	32, 31, 46, 43, 24, 24, 19, 19, 30, 30,
	25, 25, 25, 25, 25, 25, 25, 25, 33, 33,
	28, 28, 28, 28, 28, 28, 28, 27, 27, 27,
	27, 27, 27, 26, 26, 26, 26, 29, 29, 6,
	6, 7, 17, 17,
}

var yyR2 = [...]int{
	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 4, 2, 3, 3,
	9, 7, 0, 1, 3, 4, 0, 5, 1, 3,
	2, 0, 1, 1, 0, 2, 1, 0, 3, 4,
	1, 1, 1, 1, 4, 1, 1, 1, 1, 0,
	2, 11, 0, 1, 0, 1, 4, 1, 3, 0,
	1, 3, 3, 3, 3, 5, 7, 10, 5, 1,
	3, 3, 4, 6, 1, 1, 1, 3, 0, 2,
	3, 3, 3, 3, 3, 3, 3, 2, 1, 3,
	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 0, 2, 4, 4, 1, 1, 1,
	1, 1, 1, 1,
}

var yyChk = [...]int{
	-1000, -34, -35, -36, -37, -38, -39, -40, -41, -42,
	-43, -44, -45, -46, -47, 40, 43, 41, 48, 54,
	56, 57, 42, -35, 31, 31, 31, 31, 31, 31,
	31, 31, 31, 31, 31, 31, 44, 45, -16, 21,
	44, 44, 45, 46, -24, 32, -18, 5, 55, 66,
	50, -17, 72, 4, 5, 65, 66, 46, 65, 65,
	66, 68, 50, 29, 66, 24, -17, 33, 28, 68,
	16, -19, -17, 5, 53, 28, -32, -31, 5, -30,
	49, -17, -2, -1, 5, 16, 66, -30, 29, 28,
	-18, -30, 29, 34, -25, 28, -17, -28, 14, 72,
	-6, -7, 25, 26, 15, 69, 70, 71, 29, 30,
	-3, -4, 58, 59, 60, 62, 63, 64, 66, -26,
	51, -17, -33, -28, 30, -31, -28, 12, 13, -25,
	-27, 34, 35, 36, 37, 38, 39, -27, -25, -11,
	-1, 6, -12, 29, -16, 28, 28, -29, 69, 70,
	30, 29, 53, -25, -25, 30, -28, -17, -17, -28,
	30, 7, -13, -14, 16, -15, 14, -5, -29, 20,
	-10, -9, 5, 29, 52, -28, 28, -12, 28, 57,
	15, 30, 29, 30, -8, 8, 9, -29, -29, -33,
	-10, 17, 18, -9, -21, -20, 47, 30, 30, 19,
	-23, -22, 11, 28, 29, 10, -18, -22, -17, 30,
}

var yyDef = [...]int{
	0, -2, 1, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 52, 0, 0, 0, 0,
	0, 0, 0, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 13, 14, 0, 0, 0, 53,
	0, 0, 0, 0, 0, 74, 75, 57, 0, 0,
	0, 15, 17, 112, 113, 18, 0, 0, 19, 63,
	64, 0, 0, 0, 0, 0, 78, 0, 22, 0,
	0, 78, 76, 58, 0, 0, 78, 69, 0, 72,
	90, 16, 0, 23, 0, 0, 65, 103, 0, 90,
	0, 68, 0, 90, 79, 90, 0, 0, 90, 91,
	92, 93, 94, 95, 96, 109, 110, 111, 26, 34,
	52, 40, 41, 42, 43, 0, 45, 46, 0, 73,
	0, 77, 0, 88, 0, 70, 71, 90, 90, 0,
	90, 97, 98, 99, 100, 101, 102, 90, 87, 0,
	24, 0, 21, 37, 49, 0, 0, 104, 107, 108,
	66, 90, 0, 85, 86, 80, 81, 84, 82, 83,
	34, 0, 35, 36, 0, 25, 0, 0, 47, 48,
	0, 28, 31, 0, 0, 89, 90, 20, 0, 0,
	50, 44, 0, 54, 30, 32, 33, 105, 106, 0,
	0, 38, 0, 29, 59, 55, 0, 67, 27, 39,
	51, 60, 0, 0, 0, 0, 0, 61, 62, 56,
}

var yyTok1 = [...]int{
	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	28, 30, 32, 3, 29, 3, 33, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 31,
	35, 34, 36,
}

var yyTok2 = [...]int{
	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 37, 38, 39, 40,
	41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
	51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
	61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
	71, 72,
}

var yyTok3 = [...]int{
	0,
}

var yyErrorMessages = [...]struct {
	state int
	token int
	msg   string
}{}

//line yaccpar:1

/*	parser for yacc output	*/

var (
	yyDebug        = 0
	yyErrorVerbose = false
)

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

type yyParser interface {
	Parse(yyLexer) int
	Lookahead() int
}

type yyParserImpl struct {
	lval  yySymType
	stack [yyInitialStackSize]yySymType
	char  int
}

func (p *yyParserImpl) Lookahead() int {
	return p.char
}

func yyNewParser() yyParser {
	return &yyParserImpl{}
}

const yyFlag = -1000

func yyTokname(c int) string {
	if c >= 1 && c-1 < len(yyToknames) {
		if yyToknames[c-1] != "" {
			return yyToknames[c-1]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func yyStatname(s int) string {
	if s >= 0 && s < len(yyStatenames) {
		if yyStatenames[s] != "" {
			return yyStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func yyErrorMessage(state, lookAhead int) string {
	const TOKSTART = 4

	if !yyErrorVerbose {
		return "syntax error"
	}

	for _, e := range yyErrorMessages {
		if e.state == state && e.token == lookAhead {
			return "syntax error: " + e.msg
		}
	}

	res := "syntax error: unexpected " + yyTokname(lookAhead)

	// To match Bison, suggest at most four expected tokens.
	expected := make([]int, 0, 4)

	// Look for shiftable tokens.
	base := yyPact[state]
	for tok := TOKSTART; tok-1 < len(yyToknames); tok++ {
		if n := base + tok; n >= 0 && n < yyLast && yyChk[yyAct[n]] == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if yyDef[state] == -2 {
		i := 0
		for yyExca[i] != -1 || yyExca[i+1] != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; yyExca[i] >= 0; i += 2 {
			tok := yyExca[i]
			if tok < TOKSTART || yyExca[i+1] == 0 {
				continue
			}
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}

		// If the default action is to accept or reduce, give up.
		if yyExca[i+1] != 0 {
			return res
		}
	}

	for i, tok := range expected {
		if i == 0 {
			res += ", expecting "
		} else {
			res += " or "
		}
		res += yyTokname(tok)
	}
	return res
}

func yylex1(lex yyLexer, lval *yySymType) (char, token int) {
	token = 0
	char = lex.Lex(lval)
	if char <= 0 {
		token = yyTok1[0]
		goto out
	}
	if char < len(yyTok1) {
		token = yyTok1[char]
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			token = yyTok2[char-yyPrivate]
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		token = yyTok3[i+0]
		if token == char {
			token = yyTok3[i+1]
			goto out
		}
	}

out:
	if token == 0 {
		token = yyTok2[1] /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", yyTokname(token), uint(char))
	}
	return char, token
}

func yyParse(yylex yyLexer) int {
	return yyNewParser().Parse(yylex)
}

func (yyrcvr *yyParserImpl) Parse(yylex yyLexer) int {
	var yyn int
	var yyVAL yySymType
	var yyDollar []yySymType
	_ = yyDollar // silence set and not used
	yyS := yyrcvr.stack[:]

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yyrcvr.char = -1
	yytoken := -1 // yyrcvr.char translated into internal numbering
	defer func() {
		// Make sure we report no lookahead when not parsing.
		yystate = -1
		yyrcvr.char = -1
		yytoken = -1
	}()
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yytoken), yyStatname(yystate))
	}

	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	yyn = yyPact[yystate]
	if yyn <= yyFlag {
		goto yydefault /* simple state */
	}
	if yyrcvr.char < 0 {
		yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
	}
	yyn += yytoken
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = yyAct[yyn]
	if yyChk[yyn] == yytoken { /* valid shift */
		yyrcvr.char = -1
		yytoken = -1
		yyVAL = yyrcvr.lval
		yystate = yyn
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	}

yydefault:
	/* default state action */
	yyn = yyDef[yystate]
	if yyn == -2 {
		if yyrcvr.char < 0 {
			yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && yyExca[xi+1] == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = yyExca[xi+0]
			if yyn < 0 || yyn == yytoken {
				break
			}
		}
		yyn = yyExca[xi+1]
		if yyn < 0 {
			goto ret0
		}
	}
	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			yylex.Error(yyErrorMessage(yystate, yytoken))
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf(" saw %s\n", yyTokname(yytoken))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				yyn = yyPact[yyS[yyp].yys] + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = yyAct[yyn] /* simulate a shift of "error" */
					if yyChk[yystate] == yyErrCode {
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yytoken))
			}
			if yytoken == yyEofCode {
				goto ret1
			}
			yyrcvr.char = -1
			yytoken = -1
			goto yynewstate /* try again in the same state */
		}
	}

	/* reduction by production yyn */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", yyn, yyStatname(yystate))
	}

	yynt := yyn
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= yyR2[yyn]
	// yyp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = yyR1[yyn]
	yyg := yyPgo[yyn]
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = yyAct[yyg]
	} else {
		yystate = yyAct[yyj]
		if yyChk[yystate] != -yyn {
			yystate = yyAct[yyg]
		}
	}
	// dummy call; replaced with literal code
	switch yynt {

	case 15:
		yyDollar = yyS[yypt-2 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:123
		{
			s := types.ExecFileStatement{
				FileName: yyDollar[2].str,
			}
			yylex.(*lexerWrapper).channelSend <- s
		}
	case 16:
		yyDollar = yyS[yypt-4 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:129
		{
			s := types.ExecFileStatement{
				FileName: yyDollar[2].str + "." + yyDollar[4].str,
			}
			yylex.(*lexerWrapper).channelSend <- s
		}
	case 17:
		yyDollar = yyS[yypt-2 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:135
		{
			s := types.ExecFileStatement{
				FileName: yyDollar[2].str,
			}
			yylex.(*lexerWrapper).channelSend <- s
		}
	case 18:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:144
		{
			s := types.CreateDatabaseStatement{
				DatabaseId: yyDollar[3].str,
			}
			yylex.(*lexerWrapper).channelSend <- s
		}
	case 19:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:152
		{
			s := types.UseDatabaseStatement{
				DatabaseId: yyDollar[3].str,
			}
			yylex.(*lexerWrapper).channelSend <- s
		}
	case 20:
		yyDollar = yyS[yypt-9 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:160
		{
			tmpmap := make(map[string]types.Column)
			for index, item := range yyDollar[5].cols {
				item.ColumnPos = index
				tmpmap[item.Name] = item
			}

			s := types.CreateTableStatement{
				TableName:   yyDollar[3].str,
				ColumnsMap:  tmpmap,
				PrimaryKeys: yyDollar[7].keys,
				Cluster:     yyDollar[9].clstr,
			}
			yylex.(*lexerWrapper).channelSend <- s
		}
	case 21:
		yyDollar = yyS[yypt-7 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:176
		{
			tmpmap := make(map[string]types.Column)
			for index, item := range yyDollar[5].cols {
				item.ColumnPos = index
				tmpmap[item.Name] = item
			}
			s := types.CreateTableStatement{
				TableName:  yyDollar[3].str,
				ColumnsMap: tmpmap,
				Cluster:    yyDollar[7].clstr,
			}
			yylex.(*lexerWrapper).channelSend <- s
		}
	case 22:
		yyDollar = yyS[yypt-0 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:192
		{
			yyVAL.cols = make([]types.Column, 0, 0)
		}
	case 23:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:196
		{
			yyVAL.cols = make([]types.Column, 0, 1)
			yyVAL.cols = append(yyVAL.cols, yyDollar[1].col)
		}
	case 24:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:201
		{
			yyVAL.cols = append(yyDollar[1].cols, yyDollar[3].col)
		}
	case 25:
		yyDollar = yyS[yypt-4 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:207
		{
			yyVAL.col = types.Column{Name: yyDollar[1].str, Type: yyDollar[2].coltype, Unique: yyDollar[3].flag, NotNull: yyDollar[4].flag}
		}
	case 26:
		yyDollar = yyS[yypt-0 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:212
		{
			yyVAL.keys = make([]types.Key, 0, 1)
		}
	case 27:
		yyDollar = yyS[yypt-5 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:216
		{
			yyVAL.keys = yyDollar[4].keys
		}
	case 28:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:222
		{
			yyVAL.keys = make([]types.Key, 0, 1)
			yyVAL.keys = append(yyVAL.keys, yyDollar[1].key)
		}
	case 29:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:227
		{
			yyVAL.keys = append(yyDollar[1].keys, yyDollar[3].key)
		}
	case 30:
		yyDollar = yyS[yypt-2 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:233
		{
			yyVAL.key = types.Key{Name: yyDollar[1].str, KeyOrder: yyDollar[2].keyorder}
		}
	case 31:
		yyDollar = yyS[yypt-0 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:239
		{
			yyVAL.keyorder = types.Asc
		}
	case 32:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:243
		{
			yyVAL.keyorder = types.Asc
		}
	case 33:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:247
		{
			yyVAL.keyorder = types.Desc
		}
	case 34:
		yyDollar = yyS[yypt-0 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:253
		{
			yyVAL.clstr = types.Cluster{}
		}
	case 35:
		yyDollar = yyS[yypt-2 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:257
		{
			yyVAL.clstr = yyDollar[2].clstr
		}
	case 36:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:263
		{
			yyVAL.clstr = types.Cluster{OnDelete: yyDollar[1].ondelete}
		}
	case 37:
		yyDollar = yyS[yypt-0 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:269
		{
			// default
			yyVAL.ondelete = types.NoAction
		}
	case 38:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:274
		{
			yyVAL.ondelete = types.Cascade
		}
	case 39:
		yyDollar = yyS[yypt-4 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:278
		{
			yyVAL.ondelete = types.NoAction
		}
	case 40:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:284
		{
			yyVAL.coltype = yyDollar[1].coltype
		}
	case 41:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:291
		{
			yyVAL.coltype = types.ColumnType{TypeTag: types.Bool, Length: 1}
		}
	case 42:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:295
		{
			yyVAL.coltype = types.ColumnType{TypeTag: types.Int64, Length: 8}
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:299
		{
			yyVAL.coltype = types.ColumnType{TypeTag: types.Float64, Length: 8}
		}
	case 44:
		yyDollar = yyS[yypt-4 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:304
		{
			yyVAL.coltype = types.ColumnType{TypeTag: types.Bytes, Length: yyDollar[3].int}
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:308
		{
			yyVAL.coltype = types.ColumnType{TypeTag: types.Date, Length: 5}
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:312
		{
			yyVAL.coltype = types.ColumnType{TypeTag: types.Timestamp, Length: 8}
		}
	case 47:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:318
		{
			yyVAL.int = yyDollar[1].int
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:322
		{
			yyVAL.int = 255
		}
	case 49:
		yyDollar = yyS[yypt-0 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:344
		{
			yyVAL.flag = types.False
		}
	case 50:
		yyDollar = yyS[yypt-2 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:348
		{
			yyVAL.flag = types.True
		}
	case 51:
		yyDollar = yyS[yypt-11 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:354
		{
			s := types.CreateIndexStatement{
				Unique:        yyDollar[2].flag,
				IndexName:     yyDollar[4].str,
				TableName:     yyDollar[6].str,
				Keys:          yyDollar[8].keys,
				StoringClause: yyDollar[10].stcls,
				Interleaves:   yyDollar[11].intlrs,
			}
			yylex.(*lexerWrapper).channelSend <- s
		}
	case 52:
		yyDollar = yyS[yypt-0 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:368
		{
			yyVAL.flag = types.False
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:372
		{
			yyVAL.flag = types.True
		}
	case 54:
		yyDollar = yyS[yypt-0 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:379
		{
			yyVAL.stcls = types.StoringClause{}
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:383
		{
			yyVAL.stcls = yyDollar[1].stcls
		}
	case 56:
		yyDollar = yyS[yypt-4 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:389
		{
			yyVAL.stcls = types.StoringClause{ColumnNames: yyDollar[3].strs}
		}
	case 57:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:395
		{
			yyVAL.strs = make([]string, 0, 1)
			yyVAL.strs = append(yyVAL.strs, yyDollar[1].str)
		}
	case 58:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:400
		{
			yyVAL.strs = append(yyDollar[1].strs, yyDollar[3].str)
		}
	case 59:
		yyDollar = yyS[yypt-0 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:406
		{
			yyVAL.intlrs = make([]types.Interleave, 0, 0)
		}
	case 60:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:410
		{
			yyVAL.intlrs = make([]types.Interleave, 0, 1)
			yyVAL.intlrs = append(yyVAL.intlrs, yyDollar[1].intlr)
		}
	case 61:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:415
		{
			yyVAL.intlrs = append(yyDollar[1].intlrs, yyDollar[3].intlr)
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:421
		{
			yyVAL.intlr = types.Interleave{TableName: yyDollar[3].str}
		}
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:427
		{
			s := types.DropDatabaseStatement{
				DatabaseId: yyDollar[3].str,
			}
			yylex.(*lexerWrapper).channelSend <- s
		}
	case 64:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:436
		{
			s := types.DropTableStatement{
				TableName: yyDollar[3].str,
			}
			yylex.(*lexerWrapper).channelSend <- s
		}
	case 65:
		yyDollar = yyS[yypt-5 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:445
		{
			s := types.DropIndexStatement{
				TableName: yyDollar[5].str,
				IndexName: yyDollar[3].str,
			}
			yylex.(*lexerWrapper).channelSend <- s
		}
	case 66:
		yyDollar = yyS[yypt-7 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:455
		{
			s := types.InsertStament{
				TableName:   yyDollar[3].str,
				ColumnNames: make([]string, 0, 0),
				Values:      yyDollar[6].valuetypelist,
			}
			yylex.(*lexerWrapper).channelSend <- s
		}
	case 67:
		yyDollar = yyS[yypt-10 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:464
		{
			s := types.InsertStament{
				TableName:   yyDollar[3].str,
				ColumnNames: yyDollar[5].strs,
				Values:      yyDollar[9].valuetypelist,
			}
			yylex.(*lexerWrapper).channelSend <- s
		}
	case 68:
		yyDollar = yyS[yypt-5 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:474
		{
			s := types.UpdateStament{
				TableName: yyDollar[2].str,
				SetExpr:   yyDollar[4].setexprlist,
				Where:     yyDollar[5].where,
			}
			yylex.(*lexerWrapper).channelSend <- s
		}
	case 69:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:484
		{
			yyVAL.setexprlist = make([]types.SetExpr, 0, 1)
			yyVAL.setexprlist = append(yyVAL.setexprlist, yyDollar[1].setexpr)
		}
	case 70:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:489
		{
			yyVAL.setexprlist = append(yyDollar[1].setexprlist, yyDollar[3].setexpr)
		}
	case 71:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:494
		{
			yyVAL.setexpr = types.SetExpr{
				Left:  yyDollar[1].str,
				Right: yyDollar[3].valuetype,
			}
		}
	case 72:
		yyDollar = yyS[yypt-4 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:502
		{
			s := types.DeleteStatement{
				TableName: yyDollar[3].str,
				Where:     yyDollar[4].where,
			}
			yylex.(*lexerWrapper).channelSend <- s
		}
	case 73:
		yyDollar = yyS[yypt-6 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:511
		{
			s := types.SelectStatement{
				Fields:     yyDollar[2].fieldsname,
				TableNames: yyDollar[4].strs,
				Where:      yyDollar[5].where,
				Limit:      yyDollar[6].limit,
			}
			yylex.(*lexerWrapper).channelSend <- s
		}
	case 74:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:522
		{
			yyVAL.fieldsname = types.FieldsName{
				SelectAll: true,
			}
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:528
		{
			yyVAL.fieldsname = types.FieldsName{
				SelectAll:   false,
				ColumnNames: yyDollar[1].strs,
			}
		}
	case 76:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:537
		{
			yyVAL.strs = make([]string, 0, 1)
			yyVAL.strs = append(yyVAL.strs, yyDollar[1].str)
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:542
		{
			yyVAL.strs = append(yyDollar[1].strs, yyDollar[3].str)
		}
	case 78:
		yyDollar = yyS[yypt-0 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:547
		{
			yyVAL.where = nil
		}
	case 79:
		yyDollar = yyS[yypt-2 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:551
		{
			yyVAL.where = &types.Where{Expr: yyDollar[2].expr}
		}
	case 80:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:556
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 81:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:560
		{
			yyVAL.expr = &types.ComparisonExprLSRV{Left: yyDollar[1].str, Operator: yyDollar[2].compare, Right: yyDollar[3].valuetype}
		}
	case 82:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:564
		{
			yyVAL.expr = &types.ComparisonExprLVRS{Left: yyDollar[1].valuetype, Operator: yyDollar[2].compare, Right: yyDollar[3].str}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:568
		{
			yyVAL.expr = &types.ComparisonExprLVRV{Left: yyDollar[1].valuetype, Operator: yyDollar[2].compare, Right: yyDollar[3].valuetype}
		}
	case 84:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:572
		{
			yyVAL.expr = &types.ComparisonExprLSRS{Left: yyDollar[1].str, Operator: yyDollar[2].compare, Right: yyDollar[3].str}
		}
	case 85:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:576
		{
			left := yyDollar[1].expr
			right := yyDollar[3].expr
			yyVAL.expr = &types.AndExpr{Left: left, Right: right, LeftNum: left.GetTargetColsNum(), RightNum: right.GetTargetColsNum()}
		}
	case 86:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:582
		{
			left := yyDollar[1].expr
			right := yyDollar[3].expr
			yyVAL.expr = &types.OrExpr{Left: left, Right: right, LeftNum: left.GetTargetColsNum(), RightNum: right.GetTargetColsNum()}
		}
	case 87:
		yyDollar = yyS[yypt-2 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:588
		{
			left := yyDollar[2].expr
			yyVAL.expr = &types.NotExpr{Expr: left, LeftNum: left.GetTargetColsNum()}
		}
	case 88:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:595
		{
			yyVAL.valuetypelist = make([]Value.Value, 0, 1)
			yyVAL.valuetypelist = append(yyVAL.valuetypelist, yyDollar[1].valuetype)
		}
	case 89:
		yyDollar = yyS[yypt-3 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:600
		{
			yyVAL.valuetypelist = append(yyDollar[1].valuetypelist, yyDollar[3].valuetype)
		}
	case 90:
		yyDollar = yyS[yypt-0 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:605
		{
			yyVAL.valuetype = Value.Bytes{}
		}
	case 91:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:609
		{
			yyVAL.valuetype = Value.Bytes{Val: []byte(yyDollar[1].str)}
		}
	case 92:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:613
		{
			yyVAL.valuetype = Value.Int{Val: yyDollar[1].i64}
		}
	case 93:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:617
		{
			yyVAL.valuetype = Value.Float{Val: yyDollar[1].f64}
		}
	case 94:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:621
		{
			yyVAL.valuetype = Value.Bool{Val: true}
		}
	case 95:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:625
		{
			yyVAL.valuetype = Value.Bool{Val: false}
		}
	case 96:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:629
		{
			yyVAL.valuetype = Value.Null{}
		}
	case 97:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:633
		{
			yyVAL.compare = Value.Equal
		}
	case 98:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:634
		{
			yyVAL.compare = Value.Less
		}
	case 99:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:635
		{
			yyVAL.compare = Value.Great
		}
	case 100:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:636
		{
			yyVAL.compare = Value.LessEqual
		}
	case 101:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:637
		{
			yyVAL.compare = Value.GreatEqual
		}
	case 102:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:638
		{
			yyVAL.compare = Value.NotEqual
		}
	case 103:
		yyDollar = yyS[yypt-0 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:642
		{
			yyVAL.limit = types.Limit{}
		}
	case 104:
		yyDollar = yyS[yypt-2 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:646
		{
			yyVAL.limit = types.Limit{Rowcount: yyDollar[2].int}
		}
	case 105:
		yyDollar = yyS[yypt-4 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:650
		{
			yyVAL.limit = types.Limit{Offset: yyDollar[2].int, Rowcount: yyDollar[4].int}
		}
	case 106:
		yyDollar = yyS[yypt-4 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:654
		{
			yyVAL.limit = types.Limit{Offset: yyDollar[2].int, Rowcount: yyDollar[4].int}
		}
	case 107:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:659
		{
			v, _ := strconv.Atoi(yyDollar[1].str)
			yyVAL.int = v
		}
	case 108:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:664
		{
			v, _ := strconv.ParseInt(yyDollar[1].str, 16, 32)
			yyVAL.int = int(v)
		}
	case 109:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:670
		{
			v, _ := strconv.ParseInt(yyDollar[1].str, 10, 64)
			yyVAL.i64 = v
		}
	case 110:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:675
		{
			v, _ := strconv.ParseInt(yyDollar[1].str, 16, 64)
			yyVAL.i64 = v
		}
	case 111:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:682
		{
			v, _ := strconv.ParseFloat(yyDollar[1].str, 0)
			yyVAL.f64 = v
		}
	case 112:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:689
		{
			yyVAL.str = yyDollar[1].str
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
//line src/Interpreter/parser/spanner.go.y:693
		{
			yyVAL.str = yyDollar[1].str
		}
	}
	goto yystack /* stack new state and value */
}
