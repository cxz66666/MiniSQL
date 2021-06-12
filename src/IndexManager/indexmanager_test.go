package IndexManager

import (
	"minisql/src/Interpreter/value"
	"testing"
)

func TestCreateFile(t *testing.T) {
	index_info := IndexInfo{
		table_name: "student",
		attr_name:  "name",
		attr_type:  value.StringType,
	}
	Create(index_info, 10, 10)
}
