package CatalogManager

import (
	"errors"
	"fmt"
	"minisql/src/Interpreter/types"
)
//TODO NULL CHECK, if a value is null, now we can't check it!
func InsertCheck(statement types.InsertStament) (error,[]int) {
	var table *TableCatalog
	columnPositions:=make([]int,0,10)
	var  ok bool
	if len(UsingDatabase.DatabaseId)==0 {
		return errors.New("no using database， please use 'use database' before Insert"),nil
	}
	if table,ok=TableCatalogMap[statement.TableName];!ok { //
		return errors.New("don't have a table named "+statement.TableName+" ,please use create to build it"),nil
	}
	if len(statement.ColumnNames)==0 { //insert all
		if len(statement.Values)!=len(table.ColumnsMap) {
			return errors.New("input numbers don't fit the column type"),nil
		}
		valueNumber:= len(statement.Values)
		for _,column:=range table.ColumnsMap {
			pos:=column.ColumnPos
			if !(pos<valueNumber && column.Type.TypeTag== statement.Values[pos].Convert2IntType()) {
				return errors.New(fmt.Sprintf("column %s need a type %d, but your input type is %s",column.Name,column.Type.TypeTag,statement.Values[pos].String())),nil
			}
		}
		for i:=0;i<len(statement.ColumnNames);i++ {
				columnPositions=append(columnPositions,i)
		}

	} else {    //insert (a,b,c)
		for index,colName:=range statement.ColumnNames {
			var col Column
			if col,ok=table.ColumnsMap[colName];!ok {
				return errors.New("don't have a column named "+colName+" ,please check your table"),nil
			}
			if col.Type.TypeTag!=statement.Values[index].Convert2IntType() {
				return errors.New("column "+col.Name+" have a invaild type input"),nil
			}
			columnPositions=append(columnPositions,col.ColumnPos)
		}
	}
	return nil,columnPositions
}

func DeleteCheck(statement types.DeleteStatement) (error,[]int)  {
	var table *TableCatalog
	var  ok bool
	wherePositions:=make([]int,0,10)
	var err error
	if len(UsingDatabase.DatabaseId)==0 {
		return errors.New("no using database， please use 'use database' before Insert"),nil
	}
	if table,ok=TableCatalogMap[statement.TableName];!ok { //
		return errors.New("don't have a table named "+statement.TableName+" ,please use create to build it"),nil
	}
	err,wherePositions=whereOptCheck(statement.Where,table)
	if err!=nil {
		return err,nil
	}
	return nil,wherePositions
}
func UpdateCheck(statement types.UpdateStament)  (error,[]int,[]int)  {
	var table *TableCatalog
	var  ok bool
	setExprPosition:=make([]int,0,10)
	wherePositions:=make([]int,0,10) 
	var err error
	if len(UsingDatabase.DatabaseId)==0 {
		return errors.New("no using database， please use 'use database' before Insert"),nil,nil
	}
	if table,ok=TableCatalogMap[statement.TableName];!ok { //
		return errors.New("don't have a table named "+statement.TableName+" ,please use create to build it"),nil,nil
	}
	err,wherePositions=whereOptCheck(statement.Where,table)
	if err!=nil {
		return err,nil,nil
	}

	// SetExpr check!!!

	var column Column
	for _,item:=range statement.SetExpr {
		if column,ok=table.ColumnsMap[item.Left];!ok {
			return errors.New("don't have a column named "+item.Left),nil,nil
		}
		if item.Right.Convert2IntType()!=column.Type.TypeTag {
			return errors.New(fmt.Sprintf("column %s need a type %d, but your input type is %s",column.Name,column.Type.TypeTag,item.Right.String())),nil,nil
		}
		setExprPosition=append(setExprPosition,column.ColumnPos)
	}
	return nil,setExprPosition,wherePositions
}

func SelectCheck(statement types.SelectStatement) (error,[]int,[]int)  {
	var table *TableCatalog
	var  ok bool
	columnPositions:=make([]int,0,10)
	wherePositions:=make([]int,0,10)
	var err error
	if len(UsingDatabase.DatabaseId)==0 {
		return errors.New("no using database， please use 'use database' before Insert"),nil,nil
	}
	for _,tablename:=range statement.TableNames {
		if table,ok=TableCatalogMap[tablename];!ok { //
			return errors.New("don't have a table named "+tablename+" ,please use create to build it"),nil,nil
		}
	}
	err,wherePositions=whereOptCheck(statement.Where,table)
	if err!=nil {
		return err,nil,nil
	}
	var column Column
	if statement.Fields.SelectAll {
		return nil,columnPositions,wherePositions
	}
	for _,item:=range statement.Fields.ColumnNames {
		if column,ok=table.ColumnsMap[item];!ok {
			return errors.New("don't have a column named "+item),nil,nil
		}
		columnPositions=append(columnPositions,column.ColumnPos)
	}
	return nil,columnPositions,wherePositions
}

func whereOptCheck(where *types.Where,table *TableCatalog) (error,[]int) {
	columnNames:=where.Expr.GetTargetCols()
	position:=make([]int,0,len(columnNames)+1)
	var targetColumn Column
	var ok bool
	 for _,item:=range columnNames {
	 	if targetColumn,ok=table.ColumnsMap[item];!ok {
	 		return errors.New("dont have a column named "+item),nil
		}
		position=append(position,targetColumn.ColumnPos)
	 }
	 return nil,position
}