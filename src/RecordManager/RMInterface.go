package RecordManager

import (
	"minisql/src/CatalogManager"
	"minisql/src/Interpreter/types"
	"minisql/src/Interpreter/value"
)


//以下操作均保证操作数据的名称、类型准确无误

//CreateTable 拿到table的名字，同时通过cm获取当前正在使用的数据库名字，创建一个自己能找到的存记录的文件
func CreateTable(tableName string) error  {
	//TODO
	return nil
}

//DropTable 拿到table的名字，同时通过cm获取当前正在使用的数据库名字，将table和table上的索引文件全部删除，不要忘了索引
func DropTable(tableName string) error  {
	//TODO
	return nil
}
//CreateIndex  传入cm中table的引用，以及index的各种属性（名称 ，unique ，key数组目前只考虑一个key，是指在哪些column上），创建完成后记得在cm的table插入索引，直接一个append newIndex到table的index数组内
func CreateIndex(table *CatalogManager.TableCatalog, newIndex CatalogManager.IndexCatalog ) error {
	//TODO
	return nil
}
//DropIndex 传入cm中table的引用，以及indexName，cm已经做过合法性校验，直接删除索引文件，同时table中的Index属性中删除该index
func DropIndex(table *CatalogManager.TableCatalog,indexName string) error  {
	//TODO
	return nil
}
//InsertRecord 传入cm中table的引用， columnPos传入插入哪些列，其值为column在table中的第几个   startBytePos 传入开始byte的集合，分别代表每个value代表的数据从哪个byte开始存（已经加上valid位和null位），values为value数组
func InsertRecord(table *CatalogManager.TableCatalog,columnPos []int,startBytePos []int,values []value.Value) error {
	return nil
}

//SelectRecord 传入select的表，需要返回的字段的名称，where表达式，这是没有索引的
//如果column为空，就认为是选择所有
func SelectRecord(table *CatalogManager.TableCatalog,columns []string, where *types.Where) (error,[]value.Row) {
	//TODO
	//where maybe nil!!!!
	return nil,make([]value.Row,0)
}
//SelectRecordWithIndex  传入select的表，需要返回的字段的名称，where表达式, index为左 string 右 value 中间是判断符的struct， string保证存在索引
//如果column为空，就认为是选择所有
func SelectRecordWithIndex(table *CatalogManager.TableCatalog,columns []string,where *types.Where,index types.ComparisonExprLSRV) (error,[]value.Row) {
	//TODO
	//where maybe nil!!!!
	return nil,make([]value.Row,0)
}

//DeleteRecord 传入delete的表，where表达式,无索引  int返回删除了多少行
func DeleteRecord(table *CatalogManager.TableCatalog,where *types.Where) (error,int) {
	//TODO
	//where maybe nil!!!!
	return nil,0
}

//DeleteRecordWithIndex  传入select的表，where表达式, index为左 string 右 value 中间是判断符的struct， string保证存在索引 int返回删除了多少行
func DeleteRecordWithIndex(table *CatalogManager.TableCatalog,where *types.Where,index types.ComparisonExprLSRV) (error,int)  {
	//TODO
	//where maybe nil!!!!
	return nil,0
}

//UpdateRecord 传入update的表，准备更新的column，value数组，where参数 无索引 int返回删除了多少行
func UpdateRecord(table *CatalogManager.TableCatalog,columns []string,values []value.Value,where *types.Where) (error,int) {
	//TODO
	//where maybe nil!!!!
	return nil,0
}

//UpdateRecordWithIndex 传入update的表，准备更新的column，value数组，where参数 index为左 string 右 value 中间是判断符的struct， string保证存在索引
func UpdateRecordWithIndex(table *CatalogManager.TableCatalog,columns []string,values []value.Value,where *types.Where,index types.ComparisonExprLSRV) (error,int) {
	//TODO
	//where maybe nil!!!!
	return nil,0
}
