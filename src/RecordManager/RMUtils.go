package RecordManager

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"minisql/src/BufferManager"
	//"errors"
	"minisql/src/CatalogManager"
	"minisql/src/IndexManager"
	"minisql/src/Interpreter/types"
	"minisql/src/Interpreter/value"
)


func loadFreeList(tableName string) (*list.List, error) {
	//fileName := CatalogManager.TableFilePrefix() + "_data/" + tableName + "_list"
	
	return nil, nil
}

func flushFreeList(tableName string, freeList *list.List) error {
	///fileName := CatalogManager.TableFilePrefix() + "_data/" + tableName + "_list"
	return nil
}

func getRecordData(tableName string, recordPosition dataPosition, length int) ([]byte,error) {
	block, err := BufferManager.BlockRead(CatalogManager.TableFilePrefix() + "_data/" + tableName, recordPosition.Block);
	if  err != nil {
		return nil, err
	}
	defer block.FinishRead()
	record := block.Data[int(recordPosition.Offset) * length: int(recordPosition.Offset + 1) * length]
	return record, nil
}

func setRecordData(tableName string, recordPosition dataPosition, data []byte, length int) error {
	block, err := BufferManager.BlockRead(CatalogManager.TableFilePrefix() + "_data/" +tableName, recordPosition.Block);
	if  err != nil {
		return err
	}
	block.SetDirty()
	defer block.FinishRead()

	record := block.Data[int(recordPosition.Offset) * length: int(recordPosition.Offset + 1) * length]
	copy(record, data)
	return nil
} 

func getRecord(table *CatalogManager.TableCatalog, recordPosition dataPosition) (bool, value.Row, error) {
	data, err := getRecordData(table.TableName, recordPosition, table.RecordLength);
	if err != nil{
		return false, value.Row{}, err
	} 
	nullmap := make([]bool, len(table.ColumnsMap) / 8 + 1)
	bytebuf := bytes.NewBuffer(data[: (len(table.ColumnsMap)) /8 + 1 ])
	binary.Read(bytebuf, binary.LittleEndian, &nullmap)
	if(nullmap[0] == false) {
		return false, value.Row{}, nil
	}
	record := value.Row{Values: make([]value.Value, len(table.ColumnsMap))}
	//思考顺序问题, Column是以什么顺序存储的
	for _, column := range table.ColumnsMap {
		startPos := column.StartBytesPos
		length := column.Type.Length  //这个length是给char和string和null用的，所以其他类型无用
		valueType := column.Type.TypeTag

		if nullmap[column.ColumnPos + 1] == false {
			valueType = CatalogManager.Null
		}
		if record.Values[column.ColumnPos], err = 
			value.Byte2Value(data[startPos:], valueType, length); err != nil {
				return true, value.Row{}, err
		}
	}
	return true, record, nil
}

func setRecord(table *CatalogManager.TableCatalog, recordPosition dataPosition, 
			   columnPos []int, startBytePos []int, values []value.Value) error {
	data := make([]byte,table.RecordLength)
	nullmap := make([]bool, len(table.ColumnsMap)/8 + 1)
	nullmap[0] = true
	for _, columnIndex := range(columnPos) {
		nullmap[columnIndex + 1] = true
	}
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.LittleEndian, nullmap)
	copy(data[: (len(table.ColumnsMap)) /8 +1], bytebuf.Bytes())
	for index, _ := range(columnPos) {
		tmp, err := values[index].Convert2Bytes()
		if err != nil {
			return err
		}
		copy(data[startBytePos[index] : ], tmp)
	}
	if err := setRecordData(table.TableName, recordPosition, data, table.RecordLength); err != nil {
		return err
	}
	return nil
}

func columnFilter(table *CatalogManager.TableCatalog, record value.Row, columns []string) (value.Row, error ) {
	if len(columns)==0 {  //如果select* 则使用全部的即可
		return record,nil
	}
	var ret value.Row

	for _, column := range(columns) {
		ret.Values = append(ret.Values, record.Values[table.ColumnsMap[column].ColumnPos])
	}

	return ret,nil
}

func checkRow(record value.Row,where *types.Where, colPos []int) (bool, error) {
	if len(colPos) == 0 {
		return true, nil
	}
	val := make([]value.Value,0,len(colPos))
	
	for i := 0; i <len(colPos); i++ {
		val = append(val, record.Values[colPos[i]])
	}
	return where.Expr.Evaluate(val)
}
//获取   where -> 每列所在的位置切片
func getColPos(table *CatalogManager.TableCatalog,where *types.Where) (colPos []int)  {
	if where==nil {
		colPos=make([]int,0,0)
	} else {
		cols:=where.Expr.GetTargetCols()
		colPos=make([]int,0,len(cols))
		for _,item:=range cols {
			colPos=append(colPos,table.ColumnsMap[item].ColumnPos)
		}
	}
	return
}
func deleteRecord(table *CatalogManager.TableCatalog, recordPosition dataPosition) error {
	data, err := getRecordData(table.TableName, recordPosition, table.RecordLength); 
	if err != nil {
		return err
	}
	nullmap := make([]bool, len(table.ColumnsMap) + 1)
	bytebuf := bytes.NewBuffer(data[: (len(table.ColumnsMap) +1) /8 ])
	binary.Read(bytebuf, binary.LittleEndian, &nullmap)
	nullmap[0] = true
	bytebuf = bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.LittleEndian, nullmap)
	copy(data[: (len(table.ColumnsMap) +1) /8 ], bytebuf.Bytes())

	table.RecordCnt--
	return nil
}
func updateRecordData(table *CatalogManager.TableCatalog, recordPosition dataPosition, record value.Row) error {
	data := make([]byte,table.RecordLength)
	nullmap := make([]bool, len(table.ColumnsMap)/8 + 1)
	nullmap[0] = true
	
	for i, val := range(record.Values) {
		if val.Convert2IntType() != value.NullType {
			nullmap[i + 1] = true
		}
	}
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.LittleEndian, nullmap)
	copy(data[: (len(table.ColumnsMap)) /8 +1], bytebuf.Bytes())
	for i, value := range(record.Values) {
		tmp, err := value.Convert2Bytes()
		if err != nil {
			return err
		}
		for _, col := range(table.ColumnsMap) {
			if col.ColumnPos == i {
				copy(data[col.StartBytesPos : ], tmp)
				break
			}
		}
		
	}
	if err := setRecordData(table.TableName, recordPosition, data, table.RecordLength); err != nil {
		return err
	}
	return nil
}
func updateRecord(table *CatalogManager.TableCatalog, columns []string,values []value.Value, recordPosition dataPosition) (bool, error ) {
	flag, record, err := getRecord(table, recordPosition); 
	if err != nil {
		return false, err
	}
	if flag != false {
		return false, nil
	}
	//删除旧index
	for _, indexItem := range(table.Indexs) {

		indexinfo := IndexManager.IndexInfo {
			Table_name : CatalogManager.TableFilePrefix() + "_data/" + table.TableName,
			Attr_name : indexItem.Keys[0].Name,
			Attr_type : table.ColumnsMap[indexItem.Keys[0].Name].Type.TypeTag,
			Attr_length : uint16(table.ColumnsMap[indexItem.Keys[0].Name].Type.Length)}
		val := record.Values[table.ColumnsMap[indexItem.Keys[0].Name].ColumnPos]
		if err := IndexManager.Delete(indexinfo, val); err != nil {
			return false, err
		}
	}
	//更新record
	for i, columnName := range(columns) {
		record.Values[table.ColumnsMap[columnName].ColumnPos] = values[i]
	}
	//插入新index
	for _, indexItem := range(table.Indexs) {
		indexinfo := IndexManager.IndexInfo {
			Table_name : CatalogManager.TableFilePrefix() + "_data/" + table.TableName,
			Attr_name : indexItem.Keys[0].Name,
			Attr_type : table.ColumnsMap[indexItem.Keys[0].Name].Type.TypeTag,
			Attr_length : uint16(table.ColumnsMap[indexItem.Keys[0].Name].Type.Length)}
		val := record.Values[table.ColumnsMap[indexItem.Keys[0].Name].ColumnPos]
		if err := IndexManager.Insert(indexinfo, val, recordPosition ); err != nil {
			return false, err
		}
	}
	updateRecordData(table, recordPosition, record)
	return true, nil
}