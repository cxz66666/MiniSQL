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
	"encoding/binary"
)

func getRecordData(fileName string, recordPosition dataPosition, length int) ([]byte,error) {
	
	if block, err := BufferManager.BlockRead(fileName, recordPosition.block); err != nil {
		return nil, err
	}
	defer block.FinishRead()
	record := block.Data[recordPosition.offset: recordPosition.offset + length]
	return record, nil
}

func setRecordData(fileName string, recordPosition dataPosition, data []byte, length int) error {
	if block, err := BufferManager.BlockRead(fileName, recordPosition.block); err != nil {
		return err
	}
	block.SetDirty()
	defer block.FinishRead()

	record := block.Data[recordPosition.offset: recordPosition.offset + length]
	copy(record, data)
	return nil
} 

func getRecord(table *CatalogManager.TableCatalog, recordPosition dataPosition) (bool, value.Row, error) {
	if data, err := getRecordData(table.TableName, recordPosition, table.RecordLength); err != nil{
		return err
	} 
	nullmap := make([]bool, len(table.ColumnsMap) + 1)
	binary.Read(data[: (len(table.ColumnsMap) +1) /8 ], binary.LittleEndian, &nullmap)
	if(nullmap[0] == 0) {
		return 0, nil, nil
	}
	record := value.Row{
		Values: make([]value.Value, len(table.ColumnsMap))
	}
	//思考顺序问题, Column是以什么顺序存储的
	for _, column in range table.ColumnsMap {
		startPos := column.StartBytesPos
		length := column.Type.Length
		valueType := column.Type.TypeTag

		if nullmap[column.ColumnPos + 1] == 0 {
			valueType = CatalogManager.Null
		}
		if record.Values[column.ColumnPos], err = 
			value.Byte2Value(data[startPos: startPos + length], valueType, length); err != nil {
				return 1, nil, err
		}
	}
	return 1, record, nil
}

func setRecord(table *CatalogManager.TableCatalog, recordPosition dataPosition, 
			   columnPos []int, startBytePos []int, values []value.Value) err {
	data := make([]byte, 0, table.RecordLength)
	nullmap := make([]bool, len(table.ColumnsMap) + 1)
	nullmap[0] = 1
	for _, columnIndex := range(columnPos) {
		nullmap[columnIndex + 1] = 1
	}
	binary.Write(data, binary.LittleEndian, nullmap)
	for index, column := range(columnPos) {
		copy(data[startBytePos[index] : ], values[index].Convert2Bytes())
	}
	if err := setRecordData(table.TableName, recordPosition, data, table.RecordLength); err != nil {
		return err
	}
	return nil
}

func columnFilter(table *CatalogManager.TableCatalog, record value.Row, columns []string) (value.Row, err ) {
	var ret value.Row

	for _, column := range(columns) {
		ret.Value.append(record.Value[table.ColumnsMap[column].columnPos])
	}

	return ret,nil
}

func checkRow(table *CatalogManager.TableCatalog, record value.Row, where *types.Where) (bool, err) {
	if where == nil {
		return 1, nil
	}
	val := []value.Value{}
	
	for i := 0; i <where.GetTargetColsNum; i++ {
		colPos := table.ColumnsMap[where.GetTargetCols()[i]].columnPos
		val.append(record.Values[colPos])
	}
	return where.Evaluate(val)
}
func deleteRecord(table *CatalogManager.TableCatalog, recordPosition dataPosition) error {
	if data, err := getRecordData(table.TableName, recordPosition, table.RecordLength); err != nil {
		return err
	}
	nullmap := make([]bool, len(table.ColumnsMap) + 1)
	binary.Read(data[: (len(table.ColumnsMap) +1) /8 ], binary.LittleEndian, &nullmap)
	nullmap[0] = 1
	binary.Write(data[: (len(table.ColumnsMap) +1) /8 ], binary.LittleEndian, nullmap)

	table.RecordCnt--
}