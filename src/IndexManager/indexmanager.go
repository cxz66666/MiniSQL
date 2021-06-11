package IndexManager

import (
	"minisql/src/Interpreter/value"
	"os"
)

// 块中的位置
type Position struct {
	block  int
	offset int
}

/*
 * 由 IndexInfo 唯一确定了一个 index，
 * 因为我没法调用 CM，感觉这些都是必要的？
 * table_name:		表名
 * attr_name:		索引属性名
 * attr_type:		索引属性类型
 * pos_in_record:	索引在记录中的 offset（也就是记录开始的地址加上这个 offset 等于这个属性开始的地址
 */

type IndexInfo struct {
	table_name    string
	attr_name     string
	attr_type     value.ValueType
	pos_in_record int
}

/*
 * info:		用于确定是哪一个索引
 * key_value:	待插入的键值
 * pos:
 */
func Insert(info IndexInfo, key_value value.Value, pos Position) error {
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

func Create(info IndexInfo) error {
	os.Open()
	return nil
}

func externalMergeSort(info IndexInfo) error {
	return nil
}
