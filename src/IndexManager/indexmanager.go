package IndexManager

import (
	"minisql/src/BufferManager"
	"minisql/src/Interpreter/value"
	"minisql/src/Utils"
	"os"
	"path/filepath"
)

//go:generate msgp
type bpNode struct {
	key_length uint16
	data       []byte
}

// 块中的位置
type Position struct {
	Block  uint16
	Offset uint16
}

type FreeList struct {
	Name      string
	Positions []Position
}

/*
 * 由 IndexInfo 唯一确定了一个 index，
 * 因为我没法调用 CM，感觉这些都是必要的？
 * table_name:		表名
 * attr_name:		索引属性名
 * attr_type:		索引属性类型
 */

type IndexInfo struct {
	Table_name  string
	Attr_name   string
	Attr_type   value.ValueType
	Attr_length uint16
}

const index_file_suffix = ".index"

/*
 * info:		用于确定是哪一个索引
 * key_value:	待插入的键值
 * pos:
 */
func Insert(info IndexInfo, key_value value.Value, pos Position) error {
	filename := info.getFileName()
	key_length := info.Attr_length

	handleRootFull(info)

	cur_node, cur_node_block := getBpNode(filename, 0, key_length)

	M := getOrder(key_length)

	for {
		n := cur_node.getSize()
		var i uint16
		for i = 0; i < n; i++ {
			if res, _ := key_value.Compare(cur_node.getKey(i, info.Attr_type), value.Less); res {
				break
			}
		}
		if cur_node.isLeaf() == 1 {
			cur_node_block.SetDirty()
			cur_node.makeSpace(i)
			cur_node.setFilePointer(i, pos)
			cur_node.setKey(i, info.Attr_type, key_value)
			cur_node.setSize(cur_node.getSize() + 1)
			cur_node_block.FinishRead()
			break
		}
		next_node_id := cur_node.getPointer(i)
		next_node, next_node_block := getBpNode(filename, next_node_id, key_length)
		if next_node.isFull(M) { // If it is full
			next_node_block.FinishRead()
			cur_node_block.SetDirty()
			cur_node.splitNode(info, i)
		} else {
			cur_node_block.FinishRead()
			cur_node = next_node
			cur_node_block = next_node_block
		}
	}

	return nil
}

func Delete(info IndexInfo, key_value value.Value) error {
	filename := info.getFileName()
	key_length := info.Attr_length

	cur_node, cur_node_block := getBpNode(filename, 0, key_length)

	M := getOrder(key_length)

	for cur_node.isLeaf() == 0 {
		n := cur_node.getSize()
		var i uint16 = 0
		for ; i < n; i++ {
			if res, _ := key_value.Compare(cur_node.getKey(i, info.Attr_type), value.Less); res {
				break
			}
		}
		next_node_id := cur_node.getPointer(i)
		next_node, next_node_block := getBpNode(filename, next_node_id, key_length)
		if next_node.isDanger(M) { // If it is in danger of lack of node
			next_node_block.FinishRead()
			cur_node_block.SetDirty()
			cur_node.saveNode(info, i)
		} else {
			cur_node_block.FinishRead()
			cur_node = next_node
			cur_node_block = next_node_block
		}
	}
	// Search in the leaf
	n := cur_node.getSize()
	var i uint16
	for i = 0; i < n; i++ {
		if res, _ := key_value.Compare(cur_node.getKey(i, info.Attr_type), value.Equal); res {
			break
		}
	}
	if i <= n {
		cur_node.shrinkSpace(i)
		cur_node.setSize(cur_node.getSize() - 1)
	}
	cur_node_block.FinishRead()
	handleRootSingle(info)
	return nil
}

type ResultNode struct {
	Pos       Position
	next_node *ResultNode
}

func (node *ResultNode) GetNext() *ResultNode {
	return node.next_node
}

func GetFirst(info IndexInfo, key_value value.Value, compare_type value.CompareType) (*ResultNode, error) {
	//fmt.Println("begin get first")
	//defer fmt.Println("end get first")
	filename := info.getFileName()
	key_length := info.Attr_length

	cur_node, cur_node_block := getBpNode(filename, 0, key_length)
	if compare_type == value.Equal || compare_type == value.GreatEqual || compare_type == value.Great {
		var i uint16
		// Find the first leaf that contains the key

		for cur_node.isLeaf() == 0 {
			n := cur_node.getSize()
			for i = 0; i < n; i++ {
				if res, _ := cur_node.getKey(i, info.Attr_type).Compare(key_value, value.Great); res {
					break
				}
			}
			next_node_id := cur_node.getPointer(i)
			cur_node_block.FinishRead()
			next_node, next_node_block := getBpNode(filename, next_node_id, key_length)
			cur_node = next_node
			cur_node_block = next_node_block
		}
	} else {
		// Get the first leaf of all leaves
		for cur_node.isLeaf() == 0 {
			next_node_id := cur_node.getPointer(0)
			cur_node_block.FinishRead()
			next_node, next_node_block := getBpNode(filename, next_node_id, key_length)
			cur_node = next_node
			cur_node_block = next_node_block
		}
	}

	var i uint16 = 0
	begin := false
	dummy_head := new(ResultNode)
	cur_result_node := dummy_head
	//fmt.Println("find the first node")
	if n := cur_node.getSize(); n != 0 {
		switch compare_type {
		case value.Equal:
			for j := uint16(0); j < n; j++ {
				res, _ := cur_node.getKey(j, info.Attr_type).Compare(key_value,compare_type)
				if res  {
					begin = true
					new_result_node := new(ResultNode)
					*new_result_node = ResultNode{
						Pos:       cur_node.getFilePointer(j),
						next_node: nil,
					}
					cur_node_block.FinishRead() //IMPORTANT!!
					return new_result_node, nil //找到了直接返回,不要忘记放锁
				}
			}

		case value.GreatEqual, value.Great:
			for j := uint16(0); j < n; j++ {
				if res, _ := cur_node.getKey(j, info.Attr_type).Compare(key_value, compare_type); res {
					begin = true
					i = j
					break
				}
			}
		case value.LessEqual, value.Less:
			if res, _ := cur_node.getKey(0, info.Attr_type).Compare(key_value, compare_type); res {
				begin = true
				i = 0

			} //如果不满足就直接返回了
		}
	}

	// Find the first node that satisfy the condition
	//for {
	//	n := cur_node.getSize()
	//	if n == 0 {
	//		break
	//	}
	//	for j := uint16(0); j < n; j++ {
	//		if res, _ := cur_node.getKey(j, info.Attr_type).Compare(key_value, compare_type); res {
	//			begin = true
	//			i = j
	//			break
	//		}
	//	}
	//	if begin {
	//		break
	//	}
	//	// Switch to the next node
	//	next_node_id := cur_node.getNext()
	//	cur_node_block.FinishRead()
	//	if next_node_id == 0 {
	//		return nil, nil
	//	}
	//	next_node, next_node_block := getBpNode(filename, next_node_id, key_length)
	//	cur_node = next_node
	//	cur_node_block = next_node_block
	//	if cur_node.isLeaf() == 0 {
	//		// Search to the end
	//		break
	//	}
	//}
	if !begin {
		cur_node_block.FinishRead() //IMPORTANT!!
		return nil, nil
	}

	for {
		end := false
		n := cur_node.getSize()
		if n == 0 {
			break
		}
		for j := i; j < n; j++ {
			if res, _ := cur_node.getKey(j, info.Attr_type).Compare(key_value, compare_type); !res {
				end = true
				break
			}
			new_result_node := new(ResultNode)
			*new_result_node = ResultNode{
				Pos:       cur_node.getFilePointer(j),
				next_node: nil,
			}
			cur_result_node.next_node = new_result_node
			cur_result_node = new_result_node
		}
		if end {
			break
		}
		// Switch to the next node
		next_node_id := cur_node.getNext()
		cur_node_block.FinishRead()
		next_node, next_node_block := getBpNode(filename, next_node_id, key_length)
		cur_node = next_node
		cur_node_block = next_node_block
		i = 0
		if next_node_id == 0 {
			// Search to the end
			break
		}
	}
	cur_node_block.FinishRead()
	return dummy_head.next_node, nil
}

// pos_in_record 索引字段在 record 中的 offset，单位为 byte
// record_length record 的长度，单位为 byte

//新建 index
func Create(info IndexInfo) error {
	// Create file
	filename := info.Table_name + "_" + info.Attr_name + index_file_suffix
	f, err := Utils.CreateFile(filename)
	defer f.Close()
	if err != nil {
		return err
	}

	root_id, _ := BufferManager.NewBlock(filename)
	root_node, root_block := getBpNode(filename, root_id, info.Attr_length)
	root_block.SetDirty()
	root_node.nodeInit()
	root_block.FinishRead()
	return nil
}

//删除 index
func Drop(info IndexInfo) error {
	// Drop file with explicit filename
	filename := info.Table_name + "_" + info.Attr_name + index_file_suffix
	if err := Utils.RemoveFile(filename); err != nil {
		//fmt.Println(err)
		return err
	}
	return nil
}

//
func DropAll(tableName string) error {
	files, err := filepath.Glob(tableName + "_*")
	if err != nil {
		return err
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			return err
		}
	}
	return nil
}
