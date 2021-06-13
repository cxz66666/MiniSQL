package IndexManager

import (
	"minisql/src/Interpreter/value"
	"testing"
)

const info = IndexInfo{
	Table_name:  "StudentInfo",
	Attr_name:   "name",
	Attr_type:   value.StringType,
	Attr_length: 10,
}

func TestIsLeaf(t *testing.T) {

}
