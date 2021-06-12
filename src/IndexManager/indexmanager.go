package IndexManager

import (
	"fmt"
	"minisql/src/BufferManager"
	"minisql/src/Interpreter/value"
	"os"
)

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
	table_name string
	attr_name  string
	attr_type  value.ValueType
}

const index_file_suffix = ".index"

/*
 * info:		用于确定是哪一个索引
 * key_value:	待插入的键值
 * pos:
 */
func Insert(info IndexInfo, key_value value.Value, pos Position) error {
	filename := info.table_name + "_" + info.attr_name + index_file_suffix
	cur, err := BufferManager.BlockRead(filename, 0)
	if err != nil {
		fmt.Println(err)
		return err
	}
	if cur[0] == 1 { // Is Leaf

	} else { // Is not leaf

	}

	return nil
}

func Delete(info IndexInfo, key_value value.Value, pos Position) error {
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
	filename := info.table_name + "_" + info.attr_name + index_file_suffix
	if _, err := os.Create(filename); err != nil {
		fmt.Println(err)
		return err
	}
	err, buffer := BufferManager.BlockRead(filename, 0)
	if err != nil {
		fmt.Println(err)
		return err
	}
	buffer[0] = 1 //  IsLeaf[root] = true
	buffer[2] = 0 //  n[root] = 0
	return nil
}
