package RecordManager

import (
	"fmt"
	"errors"
	"minisql/src/CatalogManager"
	"minisql/src/Interpreter/types"
	"minisql/src/Interpreter/value"
	"minisql/src/IndexManager"
	"minisql/src/Utils"
	"minisql/src/BufferManager"
	"container/list"
)

type dataPosition = IndexManager.Position
freeList := list.New()

//以下操作均保证操作数据的名称、类型准确无误
//删除所有以databseId开头的table文件（虽然不优雅，但是这样最简单
func DropDatabase(databaseId string) error  {
	//删除table数据文件
	if err := Utils.RemoveAll(CatalogManager.FolderPosition + CatalogManager.DatabaseNamePrefix + CatalogManager.UsingDatabase); err != nil {
		return err
	}

	//删除index文件
	//To be continued
	return nil
}
//CreateTable 拿到table的名字，同时通过cm获取当前正在使用的数据库名字，创建一个自己能找到的存记录的文件
func CreateTable(tableName string) error  {
	filePath := CatalogManager.FolderPosition + CatalogManager.DatabaseNamePrefix + CatalogManager.UsingDatabase.DatabaseId

	//test use
	filePath = filePath + "test/"
	//must be delete

	if !Utils.Exists(filePath) {
		err := Utils.CreateDir(filePath)
		if err != nil {
			return errors.New("Can't create " + CatalogManager.UsingDatabase.DatabaseId + "'s folder")
		}
	}

	filePath = filePath + tableName
	fmt.Println(filePath)


	
	//当前数据库所在文件夹已经建立
	if !Utils.Exists(filePath) {
		f, err := Utils.CreateFile(filePath)
		defer f.Close()

		if err != nil {
			return errors.New("Can't create " + tableName +"'s table file")
		}
	}else {
		//需要保证此前没有过 create tableName 的操作，否则throw error
		return errors.New(tableName+" 's table file already exist")
	}

	return nil
}

//DropTable 拿到table的名字，同时通过cm获取当前正在使用的数据库名字，将table和table上的索引文件全部删除，不要忘了索引
//没管 tablecatalog
func DropTable(tableName string) error  {
	//删除所有的 index 文件
	if err := IndexManager.DropAll(tableName); err != nil {
		return err
	}
	//删除table对应的record文件
	if err := Utils.RemoveFile(CatalogManager.FolderPosition + CatalogManager.DatabaseNamePrefix + CatalogManager.UsingDatabase.DatabaseId +
					"/" + tableName); err != nil {
		return err
	}
	return nil
}


//CreateIndex  传入cm中table的引用，以及index的各种属性（名称 ，unique ，key数组目前只考虑一个key，是指在哪些column上），创建完成后记得在cm的table插入索引，直接一个append newIndex到table的index数组内
func CreateIndex(table *CatalogManager.TableCatalog, newIndex CatalogManager.IndexCatalog ) error {
	table.Indexs.append(newIndex)

	indexColumn := table.ColumnsMap[newIndex.Keys[0].name]
	indexinfo := IndexManager.IndexInfo {
		Table_name: table.TableName,
		Attr_name: indexColumn.Name,
		Attr_type: indexColumn.Type.Length.TypeTag,
		Attr_length: indexColumn.Type.Length,
	}
	if err := IndexManager.Create(indexinfo); err != nil {
		return err
	}
	
	return nil
}
//DropIndex 传入cm中table的引用，以及indexName，cm已经做过合法性校验，直接删除索引文件，同时table中的Index属性中删除该index
func DropIndex(table *CatalogManager.TableCatalog,indexName string) error  {
	
	indexColumn := table.ColumnsMap[indexName]
	indexinfo := IndexManager.IndexInfo {
		Table_name: table.TableName,
		Attr_name: indexColumn.Name,
		Attr_type: indexColumn.Type.Length.TypeTag,
		Attr_length: indexColumn.Type.Length,
	}
	for i, index := range table.Indexs {
		if index.IndexName == indexName {
			table.Indexs[i]= table.Indexs[len(table.Indexs) - 1]
			table = table[:len(table.Indexs) - 1]
			break
		}
	}
	if err := IndexManager.Create(indexinfo); err != nil {
		return err
	}
	
	return nil
}
//InsertRecord 传入cm中table的引用， columnPos传入插入哪些列，其值为column在table中的第几个   startBytePos 传入开始byte的集合，分别代表每个value代表的数据从哪个byte开始存（已经加上valid位和null位），values为value数组
func InsertRecord(table *CatalogManager.TableCatalog,columnPos []int,startBytePos []int,values []value.Value) error {
	//首先检查 unique限制

	if len(freeList) == 0 {
		if blockId, err := BufferManager.NewBlock(table.TableName); err != nil {
			return err
		}
		for offset := 0; offset + table.RecordLength < buffer.BlockSize; offset += table.RecordLength {
			freeList.PushBack(dataPosition {
				block: blockId,
				offset: offset
			})
		}
	}

	posElement := freeList.Front()
	freeList.Remove(posElement)
	pos := posElement.Value

	if err := setRecord(table, pos, columnPos, startBytePos, values); err != nil {
		return err
	}

	//加index
	for _, index := range(table.Indexs) {
		indexinfo := IndexManager.IndexInfo {
			Table_name : table.TableName,
			Attr_name : index.Keys[0].Name,
			Attr_type : table.ColumnsMap[index.Keys[0].Name].Type.TypeTag,
			Attr_length : table.ColumnsMap[index.Keys[0].Name].Type.Length
		}
		var val value.Value
		for i, col := range(columnPos) {
			if table.ColumnsMap[index.Keys[0].Name] == col {
				val = values[i]
				break
			}
		}
		if err := Insert(indexinfo, val, pos); err != nil {
			return err
		}
	}

	//处理 table catalog
	table.RecordCnt ++
	table.RecordTotal ++ 

	return nil
}

//SelectRecord 传入select的表，需要返回的字段的名称，where表达式，这是没有索引的
//如果column为空，就认为是选择所有
func SelectRecord(table *CatalogManager.TableCatalog,columns []string, where *types.Where) (error,[]value.Row) {
	ret := []value.Row{}
	for blockId := 0; blockId < BufferManager.GetBlockNumber(table.TableName); blockId++ {
		for offset := 0; offset + table.RecordLength < buffer.BlockSize; offset += table.RecordLength {
			if buffer.BlockSize / table.recordlength * blockId + offset > table.RecordTotal{
				break
			}
			valid, record, err := getRecord(table, dataPosition {block: blockId, offset: offset})
			if !vaild {
				continue
			}
			if err != nil {
				return err
			}
			if !checkRow(table, record, where) {
				continue
			}
			ret.append(columnFilter(table, record, columns))
		} 
	}
	
	//where maybe nil!!!!
	return nil, ret
}
//SelectRecordWithIndex  传入select的表，需要返回的字段的名称，where表达式, index为左 string 右 value 中间是判断符的struct， string保证存在索引
//如果column为空，就认为是选择所有
func SelectRecordWithIndex(table *CatalogManager.TableCatalog,columns []string,where *types.Where,index types.ComparisonExprLSRV) (error,[]value.Row) {
	ret := []value.Row{}
	indexinfo := IndexManager.IndexInfo {
		Table_name : table.TableName,
		Attr_name : index.Left,
		Attr_type : table.ColumnsMap[index.Left].Type.TypeTag,
		Attr_length : table.ColumnsMap[index.Left].Type.Length
	}

	if retNode, err := IndexManager(indexinfo, index.Right, index.Operator); err != nil {
		return err
	}

	for retNode != nil {
		if record, err := getRecord(table, retNode.Pos); err != nil {
			return err
		}
		if ans, err := columnFilter(table, record , columns); err != nil {
			return err
		}
		ret.append(ans)
		retNode = retNode.next_node
	}
	//where maybe nil!!!!
	return nil, ret
}

//DeleteRecord 传入delete的表，where表达式,无索引  int返回删除了多少行
func DeleteRecord(table *CatalogManager.TableCatalog,where *types.Where) (error,int) {
	cnt := 0
	for blockId := 0; blockId < BufferManager.GetBlockNumber(table.TableName); blockId++ {
		for offset := 0; offset + table.RecordLength < buffer.BlockSize; offset += table.RecordLength {
			pos := dataPosition{
				block : blockId,
				offset : offset
			}
			if buffer.BlockSize / table.recordlength * blockId + offset > table.RecordTotal{
				break
			}
			valid, record, err := getRecord(table, dataPosition {block: blockId, offset: offset})
			if !vaild {
				continue
			}
			if err != nil {
				return err
			}
			if !checkRow(table, record, where) {
				continue
			}

			deleteRecord(table, pos)
			//处理 index删除
			cnt++
		} 
	}

	//where maybe nil!!!!
	return nil,cnt
}

//DeleteRecordWithIndex  传入select的表，where表达式, index为左 string 右 value 中间是判断符的struct， string保证存在索引 int返回删除了多少行
func DeleteRecordWithIndex(table *CatalogManager.TableCatalog,where *types.Where,index types.ComparisonExprLSRV) (error,int)  {

	indexinfo := IndexManager.IndexInfo {
		Table_name : table.TableName,
		Attr_name : index.Left,
		Attr_type : table.ColumnsMap[index.Left].Type.TypeTag,
		Attr_length : table.ColumnsMap[index.Left].Type.Length
	}
	cnt := 0 

	if retNode, err := IndexManager(indexinfo, index.Right, index.Operator); err != nil {
		return err
	}

	for retNode != nil {
		if record, err := getRecord(table, retNode.Pos); err != nil {
			return err
		}
		if err := deleteRecord(table, retNode.Pos); err != nil {
			return err
		}
		val := record.Values[table.ColumnsMap[index.Left].ColumnPos]
		BufferManager.Delete(IndexInfo, val)
		cnt ++
	}
	//where maybe nil!!!!
	return nil, cnt
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
