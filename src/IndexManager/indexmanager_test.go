package IndexManager

import (
	"minisql/src/BufferManager"
	"minisql/src/Interpreter/value"
	"os"
	"testing"
)

var info IndexInfo = IndexInfo{
	Table_name:  "student",
	Attr_name:   "name",
	Attr_type:   value.IntType,
	Attr_length: 8,
}

func initTest() {
	os.Remove("student_name.index")
	os.Create("student_name.index")
	BufferManager.InitBuffer()
	BufferManager.NewBlock(info.getFileName())
	filename := info.getFileName()
	root, root_block := getBpNode(filename, 0, info.Attr_length)
	root_block.SetDirty()
	root.nodeInit()
	root_block.FinishRead()
	BufferManager.BlockFlushAll()
}

func TestSplit(t *testing.T) {
	initTest()
}

func TestInsert(t *testing.T) {
	initTest()
	Insert(info, value.Int{Val: 1000}, Position{1, 2})
	Insert(info, value.Int{Val: 2000}, Position{3, 4})
	Insert(info, value.Int{Val: 500}, Position{5, 6})
	Insert(info, value.Int{Val: 600}, Position{7, 8})
	Insert(info, value.Int{Val: 700}, Position{9, 10})
	Insert(info, value.Int{Val: 800}, Position{11, 12})
	Insert(info, value.Int{Val: 1500}, Position{13, 14})
	Insert(info, value.Int{Val: 1300}, Position{15, 16})
	BufferManager.BlockFlushAll()
	root, root_block := getBpNode(info.getFileName(), 0, info.Attr_length)
	root.print()
	root_block.FinishRead()
	// Insert(info, "Jane", Position{3, 4})
	// Insert(info, "Mike", Position{5, 6})
}
