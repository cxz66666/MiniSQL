package IndexManager

import (
	"fmt"
	"math/rand"
	"minisql/src/BufferManager"
	"minisql/src/Interpreter/value"
	"os"
	"testing"
	"time"
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

func (node bpNode) printTree() {
	node.print(info.Attr_type)
	n := node.getSize()
	if node.isLeaf() == 0 {
		for i := uint16(0); i <= n; i++ {
			next_block_id := node.getPointer(i)
			next_node, next_node_block := getBpNode(info.getFileName(), next_block_id, info.Attr_length)
			fmt.Println("==========================")
			fmt.Println("Block id: ", next_block_id)
			next_node.printTree()
			next_node_block.FinishRead()
		}
	}
}

func printAll() {
	fmt.Println("## Print tree info ##")
	root, root_block := getBpNode(info.getFileName(), 0, info.Attr_length)
	fmt.Println("==========================")
	fmt.Println("Block id: ", 0)
	root.printTree()
	root_block.FinishRead()
}

func TestInsert(t *testing.T) {
	initTest()
	Insert(info, value.Int{Val: 1000}, Position{1, 2})
	printAll()
	Insert(info, value.Int{Val: 2000}, Position{3, 4})
	printAll()
	Insert(info, value.Int{Val: 500}, Position{5, 6})
	printAll()
	Insert(info, value.Int{Val: 600}, Position{7, 8})
	printAll()
	Insert(info, value.Int{Val: 700}, Position{9, 10}) // Split leaf
	printAll()
	Insert(info, value.Int{Val: 800}, Position{11, 12})
	printAll()
	Insert(info, value.Int{Val: 1500}, Position{13, 14})
	printAll()
	Insert(info, value.Int{Val: 1300}, Position{15, 16})
	printAll()
	Insert(info, value.Int{Val: 1400}, Position{17, 18})
	printAll()
	Insert(info, value.Int{Val: 1700}, Position{19, 20})
	printAll()
	Insert(info, value.Int{Val: 1900}, Position{21, 22})
	printAll()
	Insert(info, value.Int{Val: 2500}, Position{23, 24})
	printAll()
	Insert(info, value.Int{Val: 3400}, Position{25, 26}) // Split non-leaf
	printAll()
	Insert(info, value.Int{Val: 1750}, Position{27, 28})
	printAll()
	BufferManager.BlockFlushAll()
}

func TestInsertScale(t *testing.T) {
	initTest()
	for i := 1; i <= 1000000; i++ {
		Insert(info, value.Int{Val: int64(i)}, Position{uint16(i), uint16(i + 1)})
	}
	BufferManager.BlockFlushAll()
}

func TestDelete(t *testing.T) {
	initTest()
	var arr [100000]int
	for i := range arr {
		arr[i] = i
	}
	slice := arr[:]
	shuffle(slice)
	for i := 0; i < 100000; i++ {
		Insert(info, value.Int{Val: int64(slice[i])}, Position{uint16(2*i - 1), uint16(2 * i)})
	}
	shuffle(slice)
	for i := 0; i < 100000; i++ {
		Delete(info, value.Int{Val: int64(slice[i])})
	}
	printAll()
}

func TestSearch(t *testing.T) {
	initTest()
	for i := 1; i <= 12; i++ {
		Insert(info, value.Int{Val: int64(i)}, Position{uint16(2*i - 1), uint16(2 * i)})
	}
	printAll()
	header, _ := GetFirst(info, value.Int{Val: 1}, value.Great)
	for header != nil {
		fmt.Println(header.Pos)
		header = header.GetNext()
	}
}

// 洗牌算法
func shuffle(arr []int) {
	rand.Seed(time.Now().UnixNano())
	var i, j int
	var temp int
	for i = len(arr) - 1; i > 0; i-- {
		j = rand.Intn(i)
		temp = arr[i]
		arr[i] = arr[j]
		arr[j] = temp
	}
}
