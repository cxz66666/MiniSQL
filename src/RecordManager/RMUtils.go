package RecordManager

import (
	//"errors"
	"minisql/src/CatalogManager"
	"minisql/src/Interpreter/types"
	"minisql/src/Interpreter/value"
	"minisql/src/BufferManager"
	"encoding/binary"
	"bytes"
)

func getRecordData(fileName string, recordPosition dataPosition, length int) ([]byte,error) {
	block, err := BufferManager.BlockRead(fileName, recordPosition.Block);
	if  err != nil {
		return nil, err
	}
	defer block.FinishRead()
	record := block.Data[int(recordPosition.Offset): int(recordPosition.Offset) + length]
	return record, nil
}

func setRecordData(fileName string, recordPosition dataPosition, data []byte, length int) error {
	block, err := BufferManager.BlockRead(fileName, recordPosition.Block);
	if  err != nil {
		return err
	}
	block.SetDirty()
	defer block.FinishRead()

	record := block.Data[int(recordPosition.Offset): int(recordPosition.Offset) + length]
	copy(record, data)
	return nil
} 

func getRecord(table *CatalogManager.TableCatalog, recordPosition dataPosition) (bool, value.Row, error) {
	data, err := getRecordData(table.TableName, recordPosition, table.RecordLength);
	if err != nil{
		return false, value.Row{}, err
	} 
	nullmap := make([]bool, len(table.ColumnsMap) + 1)
	bytebuf := bytes.NewBuffer(data[: (len(table.ColumnsMap) +1) /8 ])
	binary.Read(bytebuf, binary.LittleEndian, &nullmap)
	if(nullmap[0] == false) {
		return false, value.Row{}, nil
	}
	record := value.Row{Values: make([]value.Value, len(table.ColumnsMap))}
	//思考顺序问题, Column是以什么顺序存储的
	for _, column := range table.ColumnsMap {
		startPos := column.StartBytesPos
		length := column.Type.Length
		valueType := column.Type.TypeTag

		if nullmap[column.ColumnPos + 1] == false {
			valueType = CatalogManager.Null
		}
		if record.Values[column.ColumnPos], err = 
			value.Byte2Value(data[startPos: startPos + length], valueType, length); err != nil {
				return true, value.Row{}, err
		}
	}
	return true, record, nil
}

func setRecord(table *CatalogManager.TableCatalog, recordPosition dataPosition, 
			   columnPos []int, startBytePos []int, values []value.Value) error {
	data := make([]byte,table.RecordLength)
	nullmap := make([]bool, len(table.ColumnsMap) + 1)
	nullmap[0] = true
	for _, columnIndex := range(columnPos) {
		nullmap[columnIndex + 1] = true
	}
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.LittleEndian, nullmap)
	copy(data[: (len(table.ColumnsMap) +1) /8 ], bytebuf.Bytes())
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
	var ret value.Row

	for _, column := range(columns) {
		ret.Values = append(ret.Values, record.Values[table.ColumnsMap[column].ColumnPos])
	}

	return ret,nil
}

func checkRow(table *CatalogManager.TableCatalog, record value.Row, where *types.Where) (bool, error) {
	if where == nil {
		return true, nil
	}
	val := []value.Value{}
	
	for i := 0; i <where.Expr.GetTargetColsNum(); i++ {
		cols := where.Expr.GetTargetCols()
		colPos := table.ColumnsMap[cols[i]].ColumnPos
		val = append(val, record.Values[colPos])
	}
	return where.Expr.Evaluate(val)
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