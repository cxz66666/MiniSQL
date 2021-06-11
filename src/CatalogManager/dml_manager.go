package CatalogManager

import (
	"errors"
	"fmt"
	"minisql/src/Interpreter/types"
)
//TODO NULL CHECK, if a value is null, now we can't check it!
func InsertCheck(statement types.InsertStament) (error,[]int,[]int) {
	var table *TableCatalog
	columnPositions:=make([]int,0,10)
	startBytePos:=make([]int,0,10)
	var  ok bool
	if len(UsingDatabase.DatabaseId)==0 {
		return errors.New("no using database， please use 'use database' before Insert"),nil,nil
	}
	if table,ok=TableName2CatalogMap[statement.TableName];!ok { //
		return errors.New("don't have a table named "+statement.TableName+" ,please use create to build it"),nil,nil
	}
	if len(statement.ColumnNames)==0 { //insert all
		if len(statement.Values)!=len(table.ColumnsMap) {
			return errors.New("input numbers don't fit the column type"),nil,nil
		}
		valueNumber:= len(statement.Values)
		for _,column:=range table.ColumnsMap {
			pos:=column.ColumnPos
			if !(pos<valueNumber && column.Type.TypeTag== statement.Values[pos].Convert2IntType()) {
				return errors.New(fmt.Sprintf("column %s need a type %s, but your input value is %s",column.Name,ColumnType2StringName(column.Type.TypeTag),statement.Values[pos].String())),nil,nil
			}
			startByte:=column.StartBytesPos
			if pos>=len(startBytePos) {
				startBytePos=append(startBytePos,make([]int,pos)...)
			}
			startBytePos[pos]=startByte
		}
		for i:=0;i<len(statement.Values);i++ {
				columnPositions=append(columnPositions,i)
		}

	} else {    //insert (a,b,c)
		for index,colName:=range statement.ColumnNames {
			var col Column
			if col,ok=table.ColumnsMap[colName];!ok {
				return errors.New("don't have a column named "+colName+" ,please check your table"),nil,nil
			}
			if col.Type.TypeTag!=statement.Values[index].Convert2IntType() {
				return errors.New(fmt.Sprintf("column %s need a type %s, but your input value is %s",col.Name,ColumnType2StringName(col.Type.TypeTag),statement.Values[index].String())),nil,nil
			}
			columnPositions=append(columnPositions,col.ColumnPos)
		}
	}
	return nil,columnPositions,startBytePos
}

func DeleteCheck(statement types.DeleteStatement) (error,*types.ComparisonExprLSRV)  {
	var table *TableCatalog
	var  ok bool
	var err error
	if len(UsingDatabase.DatabaseId)==0 {
		return errors.New("no using database， please use 'use database' before Insert"),nil
	}
	if table,ok=TableName2CatalogMap[statement.TableName];!ok { //
		return errors.New("don't have a table named "+statement.TableName+" ,please use create to build it"),nil
	}
	err,exprLSRV:=whereOptCheck(statement.Where,table)
	if err!=nil {
		return err,nil
	}
	return nil,exprLSRV
}
func UpdateCheck(statement types.UpdateStament)  (error,*types.ComparisonExprLSRV)  {
	var table *TableCatalog
	var  ok bool
	var err error
	if len(UsingDatabase.DatabaseId)==0 {
		return errors.New("no using database， please use 'use database' before Insert"),nil
	}
	if table,ok=TableName2CatalogMap[statement.TableName];!ok { //
		return errors.New("don't have a table named "+statement.TableName+" ,please use create to build it"),nil
	}
	err,exprLSRV:=whereOptCheck(statement.Where,table)
	if err!=nil {
		return err,nil
	}

	// SetExpr check!!!

	var column Column
	for _,item:=range statement.SetExpr {
		if column,ok=table.ColumnsMap[item.Left];!ok {
			return errors.New("don't have a column named "+item.Left),nil
		}
		if item.Right.Convert2IntType()!=column.Type.TypeTag {
			return errors.New(fmt.Sprintf("column %s need a type %d, but your input value is %s",column.Name,column.Type.TypeTag,item.Right.String())),nil
		}
	}
	return nil,exprLSRV
}

func SelectCheck(statement types.SelectStatement) (error,*types.ComparisonExprLSRV)  {
	var table *TableCatalog
	var  ok bool

	if len(UsingDatabase.DatabaseId)==0 {
		return errors.New("no using database， please use 'use database' before Insert"),nil
	}
	for _,tablename:=range statement.TableNames {
		if table,ok=TableName2CatalogMap[tablename];!ok { //
			return errors.New("don't have a table named "+tablename+" ,please use create to build it"),nil
		}
	}
	var err, exprLSRV = whereOptCheck(statement.Where, table)
	if err!=nil {
		return err,nil
	}
	if statement.Fields.SelectAll {
		return nil,exprLSRV
	}
	for _,item:=range statement.Fields.ColumnNames {
		if _,ok=table.ColumnsMap[item];!ok {
			return errors.New("don't have a column named "+item),nil
		}
	}
	return nil,exprLSRV
}

func whereOptCheck(where *types.Where,table *TableCatalog) (error, *types.ComparisonExprLSRV ) {
	if where==nil {
		return nil,nil
	}
	columnNames:=where.Expr.GetTargetCols()
	indexList:=table.Indexs
	var ok bool
	 for _,item:=range columnNames {
	 	if _,ok=table.ColumnsMap[item];!ok {
	 		return errors.New("dont have a column named "+item),nil
		}

	 }
	for _,indexItem:=range indexList {
		if b,exprIndex:= where.Expr.GetIndexExpr(indexItem.Keys[0].Name);b {
			return nil,exprIndex
		}
	}
	 return nil,nil
}