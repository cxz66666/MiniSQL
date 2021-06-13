package IndexManager

import (
	"fmt"
	"minisql/src/BufferManager"
	"minisql/src/Interpreter/value"
	"os"
)

type bpNode []byte

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

	var cur_node bpNode
	cur_node, _ = BufferManager.BlockRead(filename, 0)

	for {
		n := cur_node.getSize()
		var i uint16 = 0
		for ; i < n; i++ {
			if res, _ := key_value.Compare(cur_node.getKey(key_length, info.Attr_type, i), value.LessEqual); res == false {
				break
			}
		}
		next_node_id := cur_node.getPointer(key_length, i)
		var next_node bpNode
		next_node, _ = BufferManager.BlockRead(filename, next_node_id)
		if next_node.getSize() == getOrder(key_length) { // If it is full
			cur_node.splitNode(info, i)
			if res, _ := key_value.Compare(cur_node.getKey(key_length, info.Attr_type, i), value.LessEqual); res == true {
				i++
				next_node_id = cur_node.getPointer(key_length, i)
				next_node, _ = BufferManager.BlockRead(filename, next_node_id)
			}
		}
		if cur_node.isLeaf() == 1 {
			ith_pointer_pos := cur_node.getPointerPosition(key_length, i)
			copy(cur_node[ith_pointer_pos+4+key_length:], cur_node[ith_pointer_pos:])
			cur_node.setFilePointer(key_length, i, pos)
			break
		}
		cur_node = next_node
	}
	return nil
}

func Delete(info IndexInfo, key_value value.Value, pos Position) error {
	filename := info.getFileName()
	key_length := info.Attr_length

	var cur_node bpNode
	cur_node, _ = BufferManager.BlockRead(filename, 0)

	for {
		n := cur_node.getSize()
		var i uint16 = 0
		for ; i < n; i++ {
			if res, _ := key_value.Compare(cur_node.getKey(key_length, info.Attr_type, i), value.LessEqual); res == false {
				break
			}
		}
		next_node_id := cur_node.getPointer(key_length, i)
		var next_node bpNode
		next_node, _ = BufferManager.BlockRead(filename, next_node_id)
		if next_node.getSize() == (getOrder(key_length)-1)/2 { // If it is in danger of lack of node
			// Save the day!
		}
		if cur_node.isLeaf() == 1 {
			ith_pointer_pos := cur_node.getPointerPosition(key_length, i)
			copy(cur_node[ith_pointer_pos:], cur_node[ith_pointer_pos+4+key_length:])
			break
		}
		cur_node = next_node
	}
	return nil
}

func GetFirst(info IndexInfo, key_value value.Value, compare_type value.CompareType) (Position, error) {
	return *new(Position), nil
}

// 返回刚才查询的结果中的下一个元素
// 如果访问越界，第二个返回值为 false
func GetNext() (Position, bool, error) {
	return *new(Position), true, nil
}

// 查询结束后，调用此函数
func Free() {

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
	buffer, err := BufferManager.BlockRead(filename, 0)
	if err != nil {
		fmt.Println(err)
		return err
	}
	buffer[0] = 1 //  IsLeaf[root] = true
	buffer[2] = 0 //  n[root] = 0
	return nil
}
