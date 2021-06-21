package CatalogManager

import (
	"errors"
	"fmt"
	"minisql/src/Interpreter/types"
	"minisql/src/Interpreter/value"
)
//Already do NULL CHECK, if a value is null, I will check it and throw a error !
func InsertCheck(statement types.InsertStament) (error,[]int,[]int) {
	var table *TableCatalog

	var  ok bool
	if len(UsingDatabase.DatabaseId)==0 {
		return errors.New("no using database， please use 'use database' before Insert"),nil,nil
	}
	if table,ok=TableName2CatalogMap[statement.TableName];!ok { //
		return errors.New("don't have a table named "+statement.TableName+" ,please use create to build it"),nil,nil
	}
	var columnPositions []int
	var startBytePos []int

	if len(statement.ColumnNames)==0 { //insert all
		if len(statement.Values)!=len(table.ColumnsMap) {
			return errors.New("input numbers don't fit the column type"),nil,nil
		}
		columnPositions=make([]int,len(statement.Values))
		startBytePos=make([]int,len(statement.Values))
		valueNumber:= len(statement.Values)
		for _,column:=range table.ColumnsMap {
			pos:=column.ColumnPos
			if !(pos<valueNumber && column.Type.TypeTag== statement.Values[pos].Convert2IntType()) {
				 if  item,ok:=statement.Values[pos].(value.Int);ok&&column.Type.TypeTag==Float64{ //是Int 同时列属性为float
					statement.Values[pos]=value.Float{Val: float64(item.Val)} //将其转为Float值
				 } else {
					 return errors.New(fmt.Sprintf("column %s need a type %s, but your input value is %s",column.Name,ColumnType2StringName(column.Type.TypeTag),statement.Values[pos].String())),nil,nil
				 }
			}
			startBytePos[column.ColumnPos]= column.StartBytesPos
		}
		for i:=0;i<len(statement.Values);i++ {
			//append 0,1,2,3...
				columnPositions[i]=i
		}

	} else {    //insert (a,b,c)
		columnPositions=make([]int,0)
		startBytePos=make([]int,0)
		for index,colName:=range statement.ColumnNames {
			var col Column
			if col,ok=table.ColumnsMap[colName];!ok {
				return errors.New("don't have a column named "+colName+" ,please check your table"),nil,nil
			}
			if col.Type.TypeTag!=statement.Values[index].Convert2IntType() {
				if  item,ok:=statement.Values[index].(value.Int);ok&&col.Type.TypeTag==Float64{ //是Int 同时列属性为float
					statement.Values[index]=value.Float{Val: float64(item.Val)} //将其转为Float值
				} else {
					return errors.New(fmt.Sprintf("column %s need a type %s, but your input value is %s",col.Name,ColumnType2StringName(col.Type.TypeTag),statement.Values[index].String())),nil,nil
				}
			}
			columnPositions=append(columnPositions,col.ColumnPos)
			startBytePos=append(startBytePos,col.StartBytesPos)
		}
		for index,col:=range table.ColumnsMap{
			if !col.NotNull {
				continue
			}
			flag:=0
			for _,colName:=range statement.ColumnNames {
				if colName==index {
					flag=1
					break
				}
			}
			if flag==0 {
				return errors.New(fmt.Sprintf("column %s is a not null type, please input a value for it",index)),nil,nil
			}
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
func UpdateCheck(statement types.UpdateStament)  (error,[]string,[]value.Value, *types.ComparisonExprLSRV)  {
	var table *TableCatalog
	var  ok bool
	var err error
	if len(UsingDatabase.DatabaseId)==0 {
		return errors.New("no using database， please use 'use database' before Insert"),nil,nil,nil
	}
	if table,ok=TableName2CatalogMap[statement.TableName];!ok { //
		return errors.New("don't have a table named "+statement.TableName+" ,please use create to build it"),nil,nil,nil
	}
	err,exprLSRV:=whereOptCheck(statement.Where,table)
	if err!=nil {
		return err,nil,nil,nil
	}

	// SetExpr check!!!
	setColumns:=make([]string,0)
	values:=make([]value.Value,0)
	var column Column
	for i,item:=range statement.SetExpr {
		if column,ok=table.ColumnsMap[item.Left];!ok {
			return errors.New("don't have a column named "+item.Left),nil,nil,nil
		}
		if item.Right.Convert2IntType()!=column.Type.TypeTag {
			if intitem,ok:=item.Right.(value.Int);ok&&column.Type.TypeTag==Float64{
				statement.SetExpr[i].Right=value.Float{Val: float64(intitem.Val) }
				item.Right=value.Float{Val: float64(intitem.Val) }
			} else {
				return errors.New(fmt.Sprintf("column %s need a type %d, but your input value is %s",column.Name,column.Type.TypeTag,item.Right.String())),nil,nil,nil
			}
		}
		setColumns=append(setColumns,item.Left)
		values=append(values,item.Right)
	}
	return nil,setColumns,values, exprLSRV
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