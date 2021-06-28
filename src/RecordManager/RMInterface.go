package RecordManager

import (
	"errors"
	"fmt"
	"minisql/src/BufferManager"
	"minisql/src/CatalogManager"
	"minisql/src/IndexManager"
	"minisql/src/Interpreter/types"
	"minisql/src/Interpreter/value"
	"minisql/src/Utils"
)

type dataNode = IndexManager.Position

//以下操作均保证操作数据的名称、类型准确无误
//删除所有以databseId开头的table文件（虽然不优雅，但是这样最简单
func DropDatabase(databaseId string) error {
	tables,err:=CatalogManager.GetDBTablesMap(databaseId) //拿到所有tablemap
	if err!=nil{
		return err
	}
	for _,item:=range tables{  //删除所有表
		err=DropTable(item.TableName)
		if err!=nil {
			return err
		}
	}
	//删除 Database 剩余的文件
	if err := Utils.RemoveAll(CatalogManager.TableFilePrefixWithDB(databaseId) + "_data"); err != nil {
		return errors.New("Can't Drop " + CatalogManager.UsingDatabase.DatabaseId + "'s folder")
	}
	return nil
}

//CreateTable 拿到table的名字，同时通过cm获取当前正在使用的数据库名字，创建一个自己能找到的存记录的文件
func CreateTable(tableName string) error {
	filePath := CatalogManager.TableFilePrefix() + "_data"

	if !Utils.Exists(filePath) {
		err := Utils.CreateDir(filePath)
		if err != nil {
			return errors.New("Can't create " + CatalogManager.UsingDatabase.DatabaseId + "'s folder")
		}
	}

	filePath = filePath + "/" + tableName
	fmt.Println(filePath)

	//保证当前数据库所在文件夹已经建立
	if !Utils.Exists(filePath) {
		f, err := Utils.CreateFile(filePath)
		defer f.Close()

		if err != nil {
			return errors.New("Can't create " + tableName + "'s table file")
		}
	} else {
		//需要保证此前没有过 create tableName 的操作，否则throw error
		return errors.New(tableName + " 's table file already exist")
	}

	return nil
}

//DropTable 拿到table的名字，同时通过cm获取当前正在使用的数据库名字，将table和table上的索引文件全部删除，不要忘了索引
//没管 tablecatalog
func DropTable(tableName string) error {
	cmname:=CatalogManager.TableFilePrefix() + "_data/" + tableName
	//删除所有的 index 文件
	if err := IndexManager.DropAll(cmname); err != nil {
		return err
	}
	//删除table对应的record文件
	if err := Utils.RemoveFile(cmname); err != nil {
		return err
	}
	//删除free list文件
	if err := Utils.RemoveFile(cmname+freeListFileHotFix); err != nil {
		return nil
	}
	if tableName==FreeList.Name{
		FreeList=IndexManager.FreeList{} //重置Freelist
	}
	//BM中删除该块
	if err:=BufferManager.DeleteOldBlock(cmname);err!=nil{
		return err
	}
	return nil
}

//CreateIndex  传入cm中table的引用，以及index的各种属性（名称 ，unique ，key数组目前只考虑一个key，是指在哪些column上），创建完成后记得在cm的table插入索引，直接一个append newIndex到table的index数组内
func CreateIndex(table *CatalogManager.TableCatalog, newIndex CatalogManager.IndexCatalog) error {
	table.Indexs = append(table.Indexs, newIndex)

	indexColumn := table.ColumnsMap[newIndex.Keys[0].Name]
	indexinfo := IndexManager.IndexInfo{
		Table_name:  CatalogManager.TableFilePrefix() + "_data/" + table.TableName,
		Attr_name:   indexColumn.Name,
		Attr_type:   indexColumn.Type.TypeTag,
		Attr_length: uint16(indexColumn.Type.Length)}
	if err := IndexManager.Create(indexinfo); err != nil {
		return err
	}

	totalBlockNum, _ := BufferManager.GetBlockNumber(CatalogManager.TableFilePrefix() + "_data/"+table.TableName)
	columnPos:=indexColumn.ColumnPos
	for blockId := uint16(0); blockId < totalBlockNum; blockId++ {
		for offset := uint16(0); (offset+1)*uint16(table.RecordLength) < BufferManager.BlockSize; offset += uint16(1) {
			if BufferManager.BlockSize/table.RecordLength*int(blockId)+int(offset) >= table.RecordTotal {
				break
			}
			pos:=dataNode{Block: blockId, Offset: offset}
			flag, record, err := getRecord(table, pos)
			if flag == false {
				continue
			}
			if err != nil {
				return err
			}
			if err = IndexManager.Insert(indexinfo, record.Values[columnPos], pos); err != nil {
				return err
			}
		}
	}
	return nil
}

//DropIndex 传入cm中table的引用，以及indexName，cm已经做过合法性校验，直接删除索引文件，同时table中的Index属性中删除该index
func DropIndex(table *CatalogManager.TableCatalog, indexName string) error {

	var indexColumn CatalogManager.Column
	//将删除table中索引放到API调CM完成
	for _, index := range table.Indexs {
		if index.IndexName == indexName {
			indexColumn = table.ColumnsMap[index.Keys[0].Name]
			//table.Indexs[i] = table.Indexs[len(table.Indexs)-1]
			//table.Indexs = table.Indexs[:len(table.Indexs)-1]
			break
		}
	}

	indexinfo := IndexManager.IndexInfo{
		Table_name:  CatalogManager.TableFilePrefix() + "_data/" + table.TableName,
		Attr_name:   indexColumn.Name,
		Attr_type:   indexColumn.Type.TypeTag,
		Attr_length: uint16(indexColumn.Type.Length)}

	if err := IndexManager.Drop(indexinfo); err != nil {
		return err
	}

	return nil
}

//InsertRecord 传入cm中table的引用， columnPos传入插入哪些列，其值为column在table中的第几个   startBytePos 传入开始byte的集合，分别代表每个value代表的数据从哪个byte开始存（已经加上valid位和null位），values为value数组
func InsertRecord(table *CatalogManager.TableCatalog, columnPos []int, startBytePos []int, values []value.Value,uniquescolumns []CatalogManager.UniquesColumn) error {
	tableNameWithPrefix:= CatalogManager.TableFilePrefix() + "_data/" + table.TableName
	//首先检查 unique限制
	err := loadFreeList(table.TableName)
	if err != nil {
		return err
	}

	indexinfo := IndexManager.IndexInfo{
		Table_name: tableNameWithPrefix,
	}
	for _,item:=range uniquescolumns {
		if item.HasIndex {
			indexinfo.Attr_name=item.ColumnName
			indexinfo.Attr_type=table.ColumnsMap[item.ColumnName].Type.TypeTag
			indexinfo.Attr_length= uint16(table.ColumnsMap[item.ColumnName].Type.Length)
			retNode, err := IndexManager.GetFirst(indexinfo,item.Value, value.Equal)
			if err != nil {
				return err
			}
			if retNode!=nil {;
				return errors.New(item.ColumnName + " uniuqe conflict")
			}
		} else {
			where:= types.Where{Expr: &types.ComparisonExprLSRV{Left: item.ColumnName,Operator: value.Equal,Right:item.Value } }//构造where表达式
			err,rows:=SelectRecord(table,make([]string,0),&where) //进行全表搜索
			if err!=nil {
				return err
			}
			if len(rows)!=0 {
				return errors.New(item.ColumnName + " uniuqe conflict")
			}
		}
	}
	//unique check legal
	if len(FreeList.Positions) == 0 {
		blockId, err := BufferManager.NewBlock(CatalogManager.TableFilePrefix() + "_data/"+table.TableName)
		if err != nil {
			return err
		}
		for offset := 0; (offset+1)*table.RecordLength < BufferManager.BlockSize; offset++ {
			FreeList.Positions = append(FreeList.Positions, dataNode{
				Block:  blockId,
				Offset: uint16(offset),
			})
		}
	}
	pos := FreeList.Positions[0]                //拿到第一个元素
	FreeList.Positions = FreeList.Positions[1:] //删除第一个元素
	//无需显式flush

	if err := setRecord(table, pos, columnPos, startBytePos, values); err != nil {
		return err
	}

	//加index
	for _,item:=range uniquescolumns {
		if item.HasIndex {
			indexinfo.Attr_name=item.ColumnName
			indexinfo.Attr_type=table.ColumnsMap[item.ColumnName].Type.TypeTag
			indexinfo.Attr_length= uint16(table.ColumnsMap[item.ColumnName].Type.Length)
			if err := IndexManager.Insert(indexinfo, item.Value, pos); err != nil {
				return err
			}
		}
	}

	//处理 table catalog
	table.RecordCnt++
	table.RecordTotal++

	return nil
}

//SelectRecord 传入select的表，需要返回的字段的名称，where表达式，这是没有索引的
//如果column为空，就认为是选择所有
func SelectRecord(table *CatalogManager.TableCatalog, columns []string, where *types.Where) (error, []value.Row) {
	ret := []value.Row{}
	colPos := getColPos(table, where)
	totalBlockNum, _ := BufferManager.GetBlockNumber(CatalogManager.TableFilePrefix() + "_data/"+table.TableName)
	for blockId := uint16(0); blockId < totalBlockNum; blockId++ {
		for offset := uint16(0); (offset+1)*uint16(table.RecordLength) < BufferManager.BlockSize; offset += uint16(1) {
			if BufferManager.BlockSize/table.RecordLength*int(blockId)+int(offset) >= table.RecordTotal {
				break
			}
			flag, record, err := getRecord(table, dataNode{Block: blockId, Offset: offset})
			if flag == false {
				continue
			}
			if err != nil {
				return err, nil
			}
			if flag, err := checkRow(record, where, colPos); err != nil || flag == false {
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
func SelectRecordWithIndex(table *CatalogManager.TableCatalog, columns []string, where *types.Where, index types.ComparisonExprLSRV) (error, []value.Row) {
	fmt.Println("index")
	ret := []value.Row{}
	indexinfo := IndexManager.IndexInfo{
		Table_name:  CatalogManager.TableFilePrefix() + "_data/" + table.TableName,
		Attr_name:   index.Left,
		Attr_type:   table.ColumnsMap[index.Left].Type.TypeTag,
		Attr_length: uint16(table.ColumnsMap[index.Left].Type.Length)}

	colPos := getColPos(table, where)

	retNode, err := IndexManager.GetFirst(indexinfo, index.Right, index.Operator)
	if err != nil {
		return err, nil
	}
	if retNode!=nil {   //初始的位置存在，则使用索引
		for retNode != nil {
			flag, record, err := getRecord(table, retNode.Pos)
			if flag == false {
				retNode = retNode.GetNext()
				continue
			}
			if err != nil {
				return err, nil
			}
			if flag, err := checkRow(record, where, colPos); err != nil || flag == false { //also need check
				if err != nil {
					return err, nil
				}
				retNode = retNode.GetNext()
				continue
			}
			ans, err := columnFilter(table, record, columns)
			if err != nil {
				return err, nil
			}
			ret = append(ret, ans)
			retNode = retNode.GetNext()
		}
		//where maybe nil!!!!
		return nil, ret
	}
	fmt.Println("not found in index")
	return  SelectRecord(table,columns,where)
}

//DeleteRecord 传入delete的表，where表达式,无索引  int返回删除了多少行
func DeleteRecord(table *CatalogManager.TableCatalog, where *types.Where) (error, int) {
	var cnt int = 0
	err := loadFreeList(table.TableName)
	if err != nil {
		return err, 0
	}
	totalBlockNum, _ := BufferManager.GetBlockNumber(CatalogManager.TableFilePrefix() + "_data/" +table.TableName)
	colPos := getColPos(table, where)
	for blockId := uint16(0); blockId < totalBlockNum; blockId++ {
		for offset := uint16(0); (offset+1)*uint16(table.RecordLength) < BufferManager.BlockSize; offset += uint16(1) {
			pos := dataNode{
				Block:  blockId,
				Offset: offset}
			if BufferManager.BlockSize/table.RecordLength*int(blockId)+int(offset) >= table.RecordTotal {
				break
			}
			flag, record, err := getRecord(table, dataNode{Block: blockId, Offset: offset})
			if flag == false {
				continue
			}
			if err != nil {
				return err, 0
			}
			if flag, err := checkRow(record, where, colPos); flag == false || err != nil {
				if err != nil {
					return err, 0
				}
				continue
			}

			if err := deleteRecord(table, pos); err != nil {
				return err, cnt
			}
			//将释放出来的空间放入队列
			FreeList.Positions = append(FreeList.Positions, pos)
			//处理 index删除
			for _, indexItem := range table.Indexs {
				indexinfo := IndexManager.IndexInfo{
					Table_name:  CatalogManager.TableFilePrefix() + "_data/" + table.TableName,
					Attr_name:   indexItem.Keys[0].Name,
					Attr_type:   table.ColumnsMap[indexItem.Keys[0].Name].Type.TypeTag,
					Attr_length: uint16(table.ColumnsMap[indexItem.Keys[0].Name].Type.Length)}
				val, err := columnFilter(table, record, []string{indexItem.Keys[0].Name})
				if err != nil {
					return err, cnt
				}
				if err := IndexManager.Delete(indexinfo, val.Values[0]); err != nil {
					return err, cnt
				}
			}
			cnt++
		}

	}
	table.RecordCnt-=cnt
	return nil, cnt
}

//DeleteRecordWithIndex  传入select的表，where表达式, index为左 string 右 value 中间是判断符的struct， string保证存在索引 int返回删除了多少行
func DeleteRecordWithIndex(table *CatalogManager.TableCatalog, where *types.Where, index types.ComparisonExprLSRV) (error, int) {
	err := loadFreeList(table.TableName)
	if err != nil {
		return err, 0
	}
	colPos := getColPos(table, where)

	indexinfo := IndexManager.IndexInfo{
		Table_name:  CatalogManager.TableFilePrefix() + "_data/" + table.TableName,
		Attr_name:   index.Left,
		Attr_type:   table.ColumnsMap[index.Left].Type.TypeTag,
		Attr_length: uint16(table.ColumnsMap[index.Left].Type.Length)}
	var cnt int = 0
	retNode, err := IndexManager.GetFirst(indexinfo, index.Right, index.Operator)
	if err != nil {
		return err, cnt
	}
	if retNode!=nil	 { //如果getFirst拿到了初始位置
		for retNode != nil {
			flag, record, err := getRecord(table, retNode.Pos)
			if flag == false {
				retNode = retNode.GetNext()
				continue
			}
			if err != nil {
				return err, cnt
			}
			if flag, err := checkRow(record, where, colPos); flag == false || err != nil {
				if err != nil {
					return err, cnt
				}
				retNode = retNode.GetNext()
				continue
			}
			if err := deleteRecord(table, retNode.Pos); err != nil {
				return err, cnt
			}
			FreeList.Positions = append(FreeList.Positions, retNode.Pos)
			for _, indexItem := range table.Indexs {
				indexinfo := IndexManager.IndexInfo{
					Table_name:  CatalogManager.TableFilePrefix() + "_data/" + table.TableName,
					Attr_name:   indexItem.Keys[0].Name,
					Attr_type:   table.ColumnsMap[indexItem.Keys[0].Name].Type.TypeTag,
					Attr_length: uint16(table.ColumnsMap[indexItem.Keys[0].Name].Type.Length)}
				val, err := columnFilter(table, record, []string{indexItem.Keys[0].Name})
				if err != nil {
					return err, cnt
				}
				if err := IndexManager.Delete(indexinfo, val.Values[0]); err != nil {
					return err, cnt
				}
			}
			retNode = retNode.GetNext()
			cnt++
		}
		//where maybe nil!!!!
		table.RecordCnt-=cnt

		return nil, cnt
	}
	//索引条件无法找到起始位置（超出边界）
	return DeleteRecord(table,where)


}

//UpdateRecord 传入update的表，准备更新的column，value数组，where参数 无索引 int返回删除了多少行
func UpdateRecord(table *CatalogManager.TableCatalog, columns []string, values []value.Value, where *types.Where) (error, int) {
	var cnt int = 0
	totalBlockNum, _ := BufferManager.GetBlockNumber(CatalogManager.TableFilePrefix() + "_data/" +table.TableName)
	colPos := getColPos(table, where)
	for blockId := uint16(0); blockId < totalBlockNum; blockId++ {
		for offset := uint16(0); (offset+1)*uint16(table.RecordLength) < BufferManager.BlockSize; offset += uint16(1) {
			pos := dataNode{
				Block:  blockId,
				Offset: offset,
			}
			if BufferManager.BlockSize/table.RecordLength*int(blockId)+int(offset) >= table.RecordTotal{
				break
			}
			flag, record, err := getRecord(table, dataNode{Block: blockId, Offset: offset})
			if flag == false {
				continue
			}
			if err != nil {
				return err, 0
			}
			if flag, err := checkRow(record, where, colPos); flag == false || err != nil {
				if err != nil {
					return err, 0
				}
				continue
			}
			bool, err := updateRecord(table, columns, values, pos)
			if err != nil {
				return err, cnt
			}
			if bool == false {
				continue
			}

			cnt++
		}
	}

	//where maybe nil!!!!
	return nil, cnt
}

//UpdateRecordWithIndex 传入update的表，准备更新的column，value数组，where参数 index为左 string 右 value 中间是判断符的struct， string保证存在索引
func UpdateRecordWithIndex(table *CatalogManager.TableCatalog, columns []string, values []value.Value, where *types.Where, index types.ComparisonExprLSRV) (error, int) {
	colPos := getColPos(table, where)

	indexinfo := IndexManager.IndexInfo{
		Table_name: CatalogManager.TableFilePrefix() + "_data/" + table.TableName,
		Attr_name:   index.Left,
		Attr_type:   table.ColumnsMap[index.Left].Type.TypeTag,
		Attr_length: uint16(table.ColumnsMap[index.Left].Type.Length)}
	var cnt int = 0
	retNode, err := IndexManager.GetFirst(indexinfo, index.Right, index.Operator)
	if err != nil {
		return err, cnt
	}
	 if retNode!=nil {
		 for retNode != nil {
			 flag, record, err := getRecord(table, retNode.Pos)
			 if flag == false {
				 retNode = retNode.GetNext()
				 continue
			 }
			 if err != nil {
				 return err, cnt
			 }
			 if flag, err := checkRow(record, where, colPos); flag == false || err != nil {
				 if err != nil {
					 return err, cnt
				 }
				 retNode = retNode.GetNext()
				 continue
			 }
			 bool, err := updateRecord(table, columns, values, retNode.Pos)
			 if err != nil {
				 return err, cnt
			 }
			 if bool == false {
				 continue
			 }

			 retNode = retNode.GetNext()
			 cnt++
		 }
		 return nil, cnt
	 }
	 return UpdateRecord(table,columns,values,where)

}
