package parser

import (
	"minisql/src/Interpreter/lexer"
	"regexp"
	"strconv"
)

const (
	LEFT_PARENTHESIS_TOKEN  = int('(')
	RIGHT_PARENTHESIS_TOKEN = int(')')
	COMMA_TOKEN             = int(',')
	SEMICOLON_TOKEN         = int(';')
	EQUAL_TOKEN             = int('=')
	ANGLE_LEFT_TOKEN        = int('<')
	ANGLE_RIGHT_TOKEN       = int('>')
	ASTERISK_TOKEN          = int('*')
	POINT_TOKEN				= int('.')
)

var keywords = map[string]int{
	"CREATE":                 CREATE,
	"create" : 				  CREATE,
	"DROP":                   DROP,
	"drop":					  DROP,
	"use": USE,
	"USE":USE,
	"DATABASE":               DATABASE,
	"database":               DATABASE,
	"TABLE":                  TABLE,
	"table": TABLE,
	"INDEX":                  INDEX,
	"index":INDEX,
	"PRIMARY":                PRIMARY,
	"primary":PRIMARY,
	"KEY":                    KEY,
	"key":KEY,
	"ASC":                    ASC,
	"asc":ASC,
	"DESC":                   DESC,
	"desc":DESC,
	"IN":                     IN,
	"in": IN,
	"NOT":                    NOT,
	"not":NOT,
	"AND": AND,
	"and":AND,
	"or":OR,
	"OR": OR,

	"STORING":STORING,
	"storing":STORING,
	"INTERLEAVE":INTERLEAVE,
	"interleave":INTERLEAVE,
	"NULL":                   NULL,
	"null":                   NULL,

	"ON":                     ON,
	"on":ON,

	"CASCADE":                CASCADE,
	"cascade":CASCADE,
	"NO":                     NO,
	"no":NO,
	"ACTION":                 ACTION,
	"MAX":                    MAX,
	"max":MAX,
	"UNIQUE":                 UNIQUE,
	"unique":UNIQUE,
	"ADD":                    ADD,
	"add":ADD,
	"COLUMN":                 COLUMN,
	"column":COLUMN,
	"SET":                    SET,
	"set":SET,
	"TRUE": TRUE,
	"true":                   TRUE,
	"FALSE":FALSE,
	"false":FALSE,
	"allow_commit_timestamp": allow_commit_timestamp,
	"BOOL":                   BOOL,
	"bool":BOOL,
	"INT64":                  INT64,
	"INT":INT64,
	"int":INT64,
	"FLOAT64":                FLOAT64,
	"float64":FLOAT64,
	"FLOAT":FLOAT64,
	"float":FLOAT64,

	"BYTES":                  BYTES,
	"bytes":BYTES,
	"char":BYTES,
	"CHAR":BYTES,
	"DATE":                   DATE,
	"date":DATE,
	"TIMESTAMP":              TIMESTAMP,
	"timestamp":TIMESTAMP,
	"database_id":            database_id,
	"decimal_value":          decimal_value,
	"hex_value":              hex_value,
	"table_name":             table_name,
	"column_name":            column_name,
	"index_name":             index_name,
	"insert":INSERT,
	"INSERT":INSERT,
	"INTO":INTO,
	"into":INTO,
	"UPDATE":UPDATE,
	"update":UPDATE,
	"DELETE":DELETE,
	"delete":DELETE,
	"SELECT":SELECT,
	"select":SELECT,
	"WHERE":WHERE,
	"where":WHERE,
	"VALUES":VALUES,
	"values":VALUES,
	"FROM":FROM,
	"from":FROM,
	"LIMIT":LIMIT,
	"limit":LIMIT,
	"OFFSET":OFFSET,
	"offset":OFFSET,
	"execfile":EXECFILE,
	"EXECFILE":EXECFILE,

}

var symbols = map[string]int{
	"(": LEFT_PARENTHESIS_TOKEN,
	")": RIGHT_PARENTHESIS_TOKEN,
	",": COMMA_TOKEN,
	";": SEMICOLON_TOKEN,
	"=": EQUAL_TOKEN,
	"<": ANGLE_LEFT_TOKEN,
	">": ANGLE_RIGHT_TOKEN,
	"*": ASTERISK_TOKEN,
	"<>":NE,
	"<=":LE,
	">=":GE,
	".":POINT_TOKEN,
}

var (
	databaseIdRegexp = regexp.MustCompile(`[a-zA-Z0-9][a-z0-9_\-]*[a-z0-9]*`)
	nameAttrRegexp   = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9_]*`)
)

type keywordTokenizer struct{}

// FromStrLit tokenize lit to a token pre-defined by goyacc with last token as a hint.
// TODO Check some literals satisfy regexp specs.
func (kt *keywordTokenizer) FromStrLit(lit string,TokenType lexer.Token, lastToken int) int {
	tokVal := 0
	switch TokenType {
	case lexer.IDENT:
		if v, ok := keywords[lit]; ok {
			tokVal = v
		} else {
			switch lastToken {
			case DATABASE:
				if databaseIdRegexp.MatchString(lit) {
					tokVal = database_id
				}
			case TABLE,INTO,UPDATE,ON:
				if nameAttrRegexp.MatchString(lit) {
					tokVal = table_name
				}
			case INDEX:
				if nameAttrRegexp.MatchString(lit) {
					tokVal = index_name
				}
			}
			if tokVal==0 {
				if nameAttrRegexp.MatchString(lit){
					tokVal=IDENT_LEGAL
				} else {
					tokVal= IDENT
				}
			}
		}

	case lexer.INTEGER:
		if _, err := strconv.ParseInt(lit, 10, 0); err == nil {
			tokVal = decimal_value
		} else if len(lit) >= 3 && lit[:2] == "0x" {
			if _, err := strconv.ParseInt(lit[2:], 16, 0); err == nil {
				tokVal = hex_value
			}
		}
	case lexer.FLOAT:
		if _, err := strconv.ParseFloat(lit, 0); err == nil {
			//fmt.Println(i)
			tokVal = float_value
		}
	case lexer.STRING:
		tokVal= string_value
	default:
		if v, ok := symbols[lit]; ok {
			tokVal = v
		}
	}

	return tokVal
}
