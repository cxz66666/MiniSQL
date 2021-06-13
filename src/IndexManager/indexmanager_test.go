package IndexManager

import (
	"minisql/src/Interpreter/value"
	"testing"
)

var info IndexInfo = IndexInfo{
	Table_name:  "student",
	Attr_name:   "name",
	Attr_type:   value.StringType,
	Attr_length: 10,
}

func TestSplit(t *testing.T) {
	BufferManager.InitBuffer()
	os.Create("../BufferManager/student_name.index")
	filename := info.getFileName()
	node_id, _ := BufferManager.NewBlock(filename)
	node, _ := getBpNode(filename, node_id, info.Attr_length)
	node.nodeInit()
	node.print()
}

func TestInsert(t *testing.T) {
	os.Create("../student_name.index")
	// Insert(info, "Hans", Position{1, 2})
	// Insert(info, "Jane", Position{3, 4})
	// Insert(info, "Mike", Position{5, 6})
}
