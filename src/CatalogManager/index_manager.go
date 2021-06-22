package CatalogManager

import (
	"errors"
	"minisql/src/Interpreter/types"
)

func CreateIndexCheck(statement types.CreateIndexStatement) (error,*IndexCatalog) {
	var table *TableCatalog
	var ok bool
	if len(UsingDatabase.DatabaseId)==0 {
		return errors.New("no using database， please use 'use database' before Insert"),nil
	}
	if table,ok=TableName2CatalogMap[statement.TableName];!ok { //
		return errors.New("don't have a table named "+statement.TableName+" ,please use create to build it"),nil
	}
	newIndexCatalog:=CreateIndexStatement2IndexCatalog(&statement)
	for _,item:=range table.Indexs {
		if item.IndexName==newIndexCatalog.IndexName{
			return errors.New("You already have a index named "+item.IndexName),nil
		}
	}
	for _,key:=range newIndexCatalog.Keys {
		if _,ok=table.ColumnsMap[key.Name];!ok {
			return errors.New("table "+table.TableName+ " don't have a column named "+key.Name),nil
		}
		if table.ColumnsMap[key.Name].Unique==false {
			return errors.New("You must create a index on a unique column."),nil
		}
	}
	return nil,newIndexCatalog
}

func DropIndexCheck(statement types.DropIndexStatement)  error  {
	var table *TableCatalog
	var ok bool
	if len(UsingDatabase.DatabaseId)==0 {
		return errors.New("no using database， please use 'use database' before Insert")
	}
	if table,ok=TableName2CatalogMap[statement.TableName];!ok { //
		return errors.New("don't have a table named "+statement.TableName+" ,please use create to build it")
	}
	for _,item:=range table.Indexs {
		if item.IndexName==statement.IndexName {
			return nil
		}
	}
	return errors.New("don't find the index named "+statement.IndexName)
}

func DropIndex(statement types.DropIndexStatement) error {
	err:=DropIndexCheck(statement)
	if err!=nil {
		return err
	}
	table:=TableName2CatalogMap[statement.TableName];
	newIndex:=make([]IndexCatalog,0,len(table.Indexs))
	for _,v:=range table.Indexs {
		if v.IndexName!=statement.IndexName {
			newIndex=append(newIndex,v)
		}
	}
	table.Indexs=newIndex
	return nil
}