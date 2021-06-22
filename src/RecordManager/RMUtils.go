package RecordManager

import (
	"errors"
	"minisql/src/BufferManager"
	"minisql/src/Utils"
	"os"

	"github.com/tinylib/msgp/msgp"

	//"errors"
	"minisql/src/CatalogManager"
	"minisql/src/IndexManager"
	"minisql/src/Interpreter/types"
	"minisql/src/Interpreter/value"
)

var FreeList IndexManager.FreeList
const freeListFileHotFix="_list"
//每次insert前都进行load
func loadFreeList(tableName string) error {
	fileName := CatalogManager.TableFilePrefix() + "_data/" + tableName + freeListFileHotFix //文件名
	if FreeList.Name == fileName {                                                  //已经load了
		return nil
	} else if len(FreeList.Name) > 0 {//需要把旧的flush
		err := FlushFreeList()
		if err != nil {
			return err
		}
	}
	if !Utils.Exists(fileName) {  //如果没有这个文件 新建该文件并序列化写入初始name信息
		newfile, err := Utils.CreateFile(fileName)
		defer newfile.Close()
		if err!=nil {
			return err
		}
		wt:=msgp.NewWriter(newfile)
		FreeList.Name=fileName
		err=FreeList.EncodeMsg(wt)
		if err!=nil	 {
			return err
		}
		return wt.Flush()
	}
	//存在该文件 直接读取即可
	existFile, err := os.Open(fileName)
	defer existFile.Close()
	if err != nil {
		return errors.New("打开free list文件失败")
	}
	rd := msgp.NewReader(existFile)
	return FreeList.DecodeMsg(rd)
}

//FlushFreeList 退出程序时候请不要忘记
func FlushFreeList() error {
	oldList, err := os.OpenFile(FreeList.Name, os.O_WRONLY|os.O_TRUNC, 0666) //写入旧文件
	defer oldList.Close()
	if err != nil {
		return errors.New("free list文件打开失败")
	}
	wt := msgp.NewWriter(oldList)
	err = FreeList.EncodeMsg(wt)
	if err != nil {
		return errors.New("free list文件写入失败")
	}
	return wt.Flush()

}

func getRecordData(tableName string, recordPosition dataNode, length int) ([]byte, error) {
	block, err := BufferManager.BlockRead(CatalogManager.TableFilePrefix()+"_data/"+tableName, recordPosition.Block)
	if err != nil {
		return nil, err
	}
	defer block.FinishRead()
	record := block.Data[int(recordPosition.Offset)*length : int(recordPosition.Offset+1)*length]
	return record, nil
}

func setRecordData(tableName string, recordPosition dataNode, data []byte, length int) error {
	block, err := BufferManager.BlockRead(CatalogManager.TableFilePrefix()+"_data/"+tableName, recordPosition.Block)
	if err != nil {
		return err
	}
	block.SetDirty()
	defer block.FinishRead()

	record := block.Data[int(recordPosition.Offset)*length : int(recordPosition.Offset+1)*length]
	copy(record, data)
	return nil
}

func getRecord(table *CatalogManager.TableCatalog, recordPosition dataNode) (bool, value.Row, error) {
	data, err := getRecordData(table.TableName, recordPosition, table.RecordLength)
	if err != nil {
		return false, value.Row{}, err
	}
	nullmapBytes:=data[0:len(table.ColumnsMap)/8+1]
	nullmap:=Utils.BytesToBools(nullmapBytes)

	if nullmap[0] == false {
		return false, value.Row{}, nil
	}
	record := value.Row{Values: make([]value.Value, len(table.ColumnsMap))}
	//思考顺序问题, Column是以什么顺序存储的
	for _, column := range table.ColumnsMap {
		startPos := column.StartBytesPos
		length := column.Type.Length //这个length是给char和string和null用的，所以其他类型无用
		valueType := column.Type.TypeTag

		if nullmap[column.ColumnPos+1] == false {
			valueType = CatalogManager.Null
		}
		if record.Values[column.ColumnPos], err =
			value.Byte2Value(data[startPos:], valueType, length); err != nil {
			return true, value.Row{}, err
		}
	}
	return true, record, nil
}


func setRecord(table *CatalogManager.TableCatalog, recordPosition dataNode,
	columnPos []int, startBytePos []int, values []value.Value) error {
	data := make([]byte, table.RecordLength)
	nullmapBytes:=data[0:len(table.ColumnsMap)/8+1]
	nullmap:=Utils.BytesToBools(nullmapBytes)
	nullmap[0] = true
	for _, columnIndex := range columnPos {
		nullmap[columnIndex+1] = true
	}
	nullmapBytes=Utils.BoolsToBytes(nullmap)

	copy(data[:], nullmapBytes)
	for index, _ := range columnPos {
		tmp, err := values[index].Convert2Bytes()
		if err != nil {
			return err
		}
		copy(data[startBytePos[index]:], tmp)
	}
	if err := setRecordData(table.TableName, recordPosition, data, table.RecordLength); err != nil {
		return err
	}
	return nil
}

func columnFilter(table *CatalogManager.TableCatalog, record value.Row, columns []string) (value.Row, error) {
	if len(columns) == 0 { //如果select* 则使用全部的即可
		return record, nil
	}
	var ret value.Row

	for _, column := range columns {
		ret.Values = append(ret.Values, record.Values[table.ColumnsMap[column].ColumnPos])
	}

	return ret, nil
}

func checkRow(record value.Row, where *types.Where, colPos []int) (bool, error) {
	if len(colPos) == 0 {
		return true, nil
	}
	val := make([]value.Value, 0, len(colPos))

	for i := 0; i < len(colPos); i++ {
		val = append(val, record.Values[colPos[i]])
	}
	return where.Expr.Evaluate(val)
}

//获取   where -> 每列所在的位置切片
func getColPos(table *CatalogManager.TableCatalog, where *types.Where) (colPos []int) {
	if where == nil {
		colPos = make([]int, 0, 0)
	} else {
		cols := where.Expr.GetTargetCols()
		colPos = make([]int, 0, len(cols))
		for _, item := range cols {
			colPos = append(colPos, table.ColumnsMap[item].ColumnPos)
		}
	}
	return
}
func deleteRecord(table *CatalogManager.TableCatalog, recordPosition dataNode) error {
	data, err := getRecordData(table.TableName, recordPosition, table.RecordLength)
	if err != nil {
		return err
	}
	nullmapBytes:=data[0:len(table.ColumnsMap)/8+1]
	nullmap:=Utils.BytesToBools(nullmapBytes)


	nullmap[0] = false //变成可用

	nullmapBytes=Utils.BoolsToBytes(nullmap)
	copy(data[:], nullmapBytes)
	setRecordData(table.TableName,recordPosition,data,table.RecordLength)
	table.RecordCnt--
	return nil
}
func updateRecordData(table *CatalogManager.TableCatalog, recordPosition dataNode, record value.Row) error {
	data := make([]byte, table.RecordLength)
	//位图
	nullmap := make([]bool, len(table.ColumnsMap)+1)
	nullmap[0] = true
  //设置为true
	for i, val := range record.Values {
		if val.Convert2IntType() != value.NullType {
			nullmap[i+1] = true
		}
	}
	//设置位bytes
	nullbytes:=Utils.BoolsToBytes(nullmap)

	copy(data[:],nullbytes)
	for i, value := range record.Values {
		tmp, err := value.Convert2Bytes()
		if err != nil {
			return err
		}
		for _, col := range table.ColumnsMap {
			if col.ColumnPos == i {
				copy(data[col.StartBytesPos:], tmp)
				break
			}
		}

	}
	if err := setRecordData(table.TableName, recordPosition, data, table.RecordLength); err != nil {
		return err
	}
	return nil
}
func updateRecord(table *CatalogManager.TableCatalog, columns []string, values []value.Value, recordPosition dataNode) (bool, error) {
	flag, record, err := getRecord(table, recordPosition)
	if err != nil {
		return false, err
	}
	if flag == false {
		return false, nil
	}
	//删除旧index
	for _, indexItem := range table.Indexs {

		indexinfo := IndexManager.IndexInfo{
			Table_name:  CatalogManager.TableFilePrefix() + "_data/" + table.TableName,
			Attr_name:   indexItem.Keys[0].Name,
			Attr_type:   table.ColumnsMap[indexItem.Keys[0].Name].Type.TypeTag,
			Attr_length: uint16(table.ColumnsMap[indexItem.Keys[0].Name].Type.Length)}
		val := record.Values[table.ColumnsMap[indexItem.Keys[0].Name].ColumnPos]
		if err := IndexManager.Delete(indexinfo, val); err != nil {
			return false, err
		}
	}
	//更新record
	for i, columnName := range columns {
		record.Values[table.ColumnsMap[columnName].ColumnPos] = values[i]
	}
	//插入新index
	for _, indexItem := range table.Indexs {
		indexinfo := IndexManager.IndexInfo{
			Table_name:  CatalogManager.TableFilePrefix() + "_data/" + table.TableName,
			Attr_name:   indexItem.Keys[0].Name,
			Attr_type:   table.ColumnsMap[indexItem.Keys[0].Name].Type.TypeTag,
			Attr_length: uint16(table.ColumnsMap[indexItem.Keys[0].Name].Type.Length)}
		val := record.Values[table.ColumnsMap[indexItem.Keys[0].Name].ColumnPos]
		if err := IndexManager.Insert(indexinfo, val, recordPosition); err != nil {
			return false, err
		}
	}
	updateRecordData(table, recordPosition, record)
	return true, nil
}
