package IndexManager

import (
	"fmt"
	"minisql/src/BufferManager"
	"minisql/src/Interpreter/value"
	"os"
)

type bpNode struct {
	key_length uint16
	data       []byte
}

// 块中的位置
type Position struct {
	block  int16
	offset int16
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

	var cur_node bpNode
	cur_node_block, _ := BufferManager.BlockRead(filename, 0)
	cur_node = bpNode{
		key_length: key_length,
		data:       cur_node_block.Data,
	}

	for {
		n := cur_node.getSize()
		var i uint16 = 0
		for ; i < n; i++ {
			if res, _ := key_value.Compare(cur_node.getKey(info.Attr_type, i), value.LessEqual); !res {
				break
			}
		}
		next_node_id := cur_node.getPointer(i)
		var next_node bpNode
		next_node_block, _ := BufferManager.BlockRead(filename, next_node_id)
		next_node = bpNode{
			key_length: key_length,
			data:       next_node_block.Data,
		}
		if next_node.getSize() == getOrder(key_length) { // If it is full
			next_node_block.FinishRead()
			cur_node_block.SetDirty()
			cur_node.splitNode(info, i)
			if res, _ := key_value.Compare(cur_node.getKey(info.Attr_type, i), value.LessEqual); res {
				i++
				next_node_id = cur_node.getPointer(i)
				next_node_block, _ = BufferManager.BlockRead(filename, next_node_id)
				next_node = bpNode{
					key_length: key_length,
					data:       next_node_block.Data,
				}
			}
		}
		if cur_node.isLeaf() == 1 {
			cur_node_block.SetDirty()
			ith_pointer_pos := cur_node.getPointerPosition(i)
			copy(cur_node.data[ith_pointer_pos+4+key_length:], cur_node.data[ith_pointer_pos:])
			cur_node.setFilePointer(i, pos)
			break
		}
		cur_node_block.FinishRead()
		cur_node = next_node
		cur_node_block = next_node_block
	}
	cur_node_block.FinishRead()
	return nil
}

func Delete(info IndexInfo, key_value value.Value, pos Position) error {
	filename := info.getFileName()
	key_length := info.Attr_length

	var cur_node bpNode
	cur_node_block, _ := BufferManager.BlockRead(filename, 0)
	cur_node = bpNode{
		key_length: key_length,
		data:       cur_node_block.Data,
	}

	for {
		n := cur_node.getSize()
		var i uint16 = 0
		for ; i < n; i++ {
			if res, _ := key_value.Compare(cur_node.getKey(info.Attr_type, i), value.LessEqual); !res {
				break
			}
		}
		next_node_id := cur_node.getPointer(i)
		var next_node bpNode
		next_node_block, _ := BufferManager.BlockRead(filename, next_node_id)
		next_node = bpNode{
			key_length: key_length,
			data:       next_node_block.Data,
		}
		if next_node.getSize() == (getOrder(key_length)-1)/2 { // If it is in danger of lack of node
			next_node_block.FinishRead()
			cur_node_block.SetDirty()
			cur_node.saveNode(info, i)
		}
		if cur_node.isLeaf() == 1 {
			ith_pointer_pos := cur_node.getPointerPosition(i)
			copy(cur_node.data[ith_pointer_pos:], cur_node.data[ith_pointer_pos+4+key_length:])
			break
		}
		cur_node_block.FinishRead()
		cur_node = next_node
		cur_node_block = next_node_block
	}
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
	filename := info.getFileName()
	key_length := info.Attr_length

	cur_node, cur_node_block := getBpNode(filename, 0, key_length)
	var i uint16
	if compare_type == value.Equal || compare_type == value.GreatEqual || compare_type == value.Great {
		// Find the first node that is great or equal to the node
		for {
			n := cur_node.getSize()
			var cur_compare_type value.CompareType
			var cur_n uint16
			if cur_node.isLeaf() == 1 {
				cur_compare_type = value.GreatEqual
				cur_n = n
			} else {
				cur_compare_type = compare_type
				cur_n = n - 1
			}
			for i = 0; i <= cur_n; i++ {
				if res, _ := key_value.Compare(cur_node.getKey(info.Attr_type, i), cur_compare_type); res {
					break
				}
			}
			if cur_node.isLeaf() == 1 {
				break
			}
			next_node_id := cur_node.getPointer(i)
			next_node, next_node_block := getBpNode(filename, next_node_id, key_length)
			cur_node_block.FinishRead()
			cur_node = next_node
			cur_node_block = next_node_block
		}
	} else {
		// Get the first node of all nodes
		for cur_node.isLeaf() == 0 {
			next_node_id := cur_node.getPointer(0)
			next_node, next_node_block := getBpNode(filename, next_node_id, key_length)
			cur_node_block.FinishRead()
			cur_node = next_node
			cur_node_block = next_node_block
		}
		i = 0
	}

	dummy_head := new(ResultNode)
	cur_result_node := dummy_head

	for {
		failed := false
		n := cur_node.getSize()
		if i > n { // Switch to the next node
			next_node_id := cur_node.getNext()
			next_node, next_node_block := getBpNode(filename, next_node_id, key_length)
			cur_node_block.FinishRead()
			cur_node = next_node
			cur_node_block = next_node_block
			i = 0
		}

		for j := i; j <= n; j++ {
			if res, _ := key_value.Compare(cur_node.getKey(info.Attr_type, j), compare_type); !res {
				failed = true
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
		if failed {
			break
		}
	}

	return dummy_head.next_node, nil
}

// pos_in_record 索引字段在 record 中的 offset，单位为 byte
// record_length record 的长度，单位为 byte
func Create(info IndexInfo, pos_in_record int, record_length int) error {
	// Create file
	filename := info.Table_name + "_" + info.Attr_name + index_file_suffix
	if _, err := os.Create(filename); err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}
