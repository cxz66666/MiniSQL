package RecordManager

import (
	"container/list"
	"errors"
	"fmt"
	"minisql/src/BufferManager"
	"minisql/src/CatalogManager"
	"minisql/src/IndexManager"
	"minisql/src/Interpreter/types"
	"minisql/src/Interpreter/value"
	"minisql/src/Utils"
)

type dataPosition = IndexManager.Position
var freeList list.List

func LoadFreeList() error {

	//freeList = list.New()
	return nil
}

//以下操作均保证操作数据的名称、类型准确无误
//删除所有以databseId开头的table文件（虽然不优雅，但是这样最简单
func DropDatabase(databaseId string) error  {
	//删除table数据文件
	if err := Utils.RemoveAll(CatalogManager.FolderPosition + CatalogManager.DatabaseNamePrefix + CatalogManager.UsingDatabase.DatabaseId); err != nil {
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
	table.Indexs = append(table.Indexs, newIndex)

	indexColumn := table.ColumnsMap[newIndex.Keys[0].Name]
	indexinfo := IndexManager.IndexInfo {
		Table_name: table.TableName,
		Attr_name: indexColumn.Name,
		Attr_type: indexColumn.Type.TypeTag,
		Attr_length: uint16(indexColumn.Type.Length)}
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
		Attr_type: indexColumn.Type.TypeTag,
		Attr_length: uint16(indexColumn.Type.Length)}
	for i, index := range table.Indexs {
		if index.IndexName == indexName {
			table.Indexs[i]= table.Indexs[len(table.Indexs) - 1]
			table.Indexs = table.Indexs[:len(table.Indexs) - 1]
			break}
	}
	if err := IndexManager.Create(indexinfo); err != nil {
		return err
	}
	
	return nil
}
//InsertRecord 传入cm中table的引用， columnPos传入插入哪些列，其值为column在table中的第几个   startBytePos 传入开始byte的集合，分别代表每个value代表的数据从哪个byte开始存（已经加上valid位和null位），values为value数组
func InsertRecord(table *CatalogManager.TableCatalog,columnPos []int,startBytePos []int,values []value.Value) error {
	//首先检查 unique限制

	
	if freeList.Len() == 0 {
		blockId, err := BufferManager.NewBlock(table.TableName);
		if  err != nil {
			return err
		}
		for offset := 0; offset + table.RecordLength < BufferManager.BlockSize; offset += table.RecordLength {
			freeList.PushBack(dataPosition {
				Block: blockId,
				Offset: uint16(offset)})
		}
	}

	posElement := freeList.Front()
	freeList.Remove(posElement)
	pos, _:= posElement.Value.(dataPosition)
	//pos := dataPosition{}
	if err := setRecord(table, pos, columnPos, startBytePos, values); err != nil {
		return err
	}

	//加index
	for _, index := range(table.Indexs) {
		indexinfo := IndexManager.IndexInfo {
			Table_name : table.TableName,
			Attr_name : index.Keys[0].Name,
			Attr_type : table.ColumnsMap[index.Keys[0].Name].Type.TypeTag,
			Attr_length :uint16(table.ColumnsMap[index.Keys[0].Name].Type.Length)}
		var val value.Value
		for i, col := range(columnPos) {
			if table.ColumnsMap[index.Keys[0].Name].ColumnPos == col {
				val = values[i]
				break
			}
		}
		if err := IndexManager.Insert(indexinfo, val, pos); err != nil {
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
	colPos:=getColPos(table,where)
	totalBlockNum, _ := BufferManager.GetBlockNumber(table.TableName)
	for blockId := uint16(0); blockId < totalBlockNum; blockId++ {
		for offset := uint16(0); offset + uint16(table.RecordLength) < BufferManager.BlockSize; offset += uint16(table.RecordLength) {
			if BufferManager.BlockSize / uint16(table.RecordLength) * blockId + offset > uint16(table.RecordTotal){
				break
			}
			flag, record, err := getRecord(table, dataPosition {Block: blockId, Offset: offset})
			if flag == false {
				continue
			}
			if err != nil {
				return err, nil
			}
			if flag, err := checkRow(record, where,colPos); err != nil || flag == false {
				if err != nil {
					return err, nil
				}
				continue
			}
			tmp, _ := columnFilter(table, record, columns)
			ret = append(ret, tmp)
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
		Attr_length : uint16(table.ColumnsMap[index.Left].Type.Length)}

	colPos:=getColPos(table,where)

	retNode, err := IndexManager.GetFirst(indexinfo, index.Right, index.Operator);
		if  err != nil {
		return err,nil
	}

	for retNode != nil {
		flag, record, err := getRecord(table, retNode.Pos);
		if flag == false {
			retNode = retNode.GetNext()
			continue
		}
		if  err != nil {
			return err, nil
		}
		if flag, err := checkRow(record, where,colPos); err != nil || flag == false {  //also need check
			if err != nil {
				return err, nil
			}
			continue
		}
		ans, err := columnFilter(table, record , columns);
		if  err != nil {
			return err, nil
		}
		ret = append(ret, ans)
		retNode = retNode.GetNext()
	}
	//where maybe nil!!!!
	return nil, ret
}

//DeleteRecord 传入delete的表，where表达式,无索引  int返回删除了多少行
func DeleteRecord(table *CatalogManager.TableCatalog,where *types.Where) (error,int) {
	var cnt int = 0
	totalBlockNum, _ := BufferManager.GetBlockNumber(table.TableName)
	colPos:=getColPos(table,where)
 	for blockId := uint16(0); blockId < totalBlockNum; blockId++ {
		for offset := uint16(0); offset + uint16(table.RecordLength) < BufferManager.BlockSize; offset += uint16(table.RecordLength) {
			pos := dataPosition{
				Block : blockId,
				Offset : offset}
			if  BufferManager.BlockSize / uint16(table.RecordLength) * blockId + offset > uint16(table.RecordTotal){
				break
			}
			flag, record, err := getRecord(table, dataPosition {Block: blockId, Offset: offset})
			if flag == false {
				continue
			}
			if err != nil {
				return err, 0
			}
			if flag, err := checkRow(record, where,colPos); flag == false || err != nil {
				if err != nil {
					return err, 0
				}
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

	colPos:=getColPos(table,where)

	indexinfo := IndexManager.IndexInfo {
		Table_name : table.TableName,
		Attr_name : index.Left,
		Attr_type : table.ColumnsMap[index.Left].Type.TypeTag,
		Attr_length : uint16(table.ColumnsMap[index.Left].Type.Length)}
	var cnt int = 0 
	retNode, err := IndexManager.GetFirst(indexinfo, index.Right, index.Operator);
	if  err != nil {
		return err, 0
	}

	for retNode != nil {
		flag, record, err := getRecord(table, retNode.Pos);
		if(flag == false) {
			retNode = retNode.GetNext()
			continue
		}
		if  err != nil {	
			return err, 0
		}
		if flag, err := checkRow(record, where,colPos); flag == false || err != nil {
			if err != nil {
				return err, 0
			}
			continue
		}
		if err := deleteRecord(table, retNode.Pos); err != nil {
			return err, 0
		}
		val := record.Values[table.ColumnsMap[index.Left].ColumnPos]
		IndexManager.Delete(indexinfo, val)
		retNode = retNode.GetNext()
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
