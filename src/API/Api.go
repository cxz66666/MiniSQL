package API

import (
	"bytes"
	"errors"
	"fmt"
	"minisql/src/CatalogManager"
	"minisql/src/Interpreter/types"
	"minisql/src/Interpreter/value"
	"minisql/src/RecordManager"
)

func HandleOneParse(statement types.DStatements) error  {
	switch statement.GetOperationType() {
	case types.CreateDatabase:
		return CreateDatabaseAPI(statement.(types.CreateDatabaseStatement))
	case types.UseDatabase:
		return UseDatabaseAPI(statement.(types.UseDatabaseStatement))
	case types.CreateTable:
		return CreateTableAPI(statement.(types.CreateTableStatement))
	case types.CreateIndex:
		return CreateIndexAPI(statement.(types.CreateIndexStatement))
	case types.DropTable:
		return DropTableAPI(statement.(types.DropTableStatement))
	case types.DropIndex:
		return DropIndexAPI(statement.(types.DropIndexStatement))
	case types.DropDatabase:
		return DropDatabaseAPI(statement.(types.DropDatabaseStatement))
	case types.Insert:
		return InsertAPI(statement.(types.InsertStament))
	case types.Update:
		return UpdateAPI(statement.(types.UpdateStament))
	case types.Delete:
		return DeleteAPI(statement.(types.DeleteStatement))
	case types.Select:
		return SelectAPI(statement.(types.SelectStatement))
	}
	return errors.New("Unresolved type for parse")
}

func CreateDatabaseAPI(statement types.CreateDatabaseStatement)  error {
	return CatalogManager.CreateDatabase(statement.DatabaseId)
}

func UseDatabaseAPI(statement types.UseDatabaseStatement) error  {
	return CatalogManager.UseDatabase(statement.DatabaseId)
}

func DropDatabaseAPI(statement types.DropDatabaseStatement) error  {
	return CatalogManager.DropDatabase(statement.DatabaseId)
}


func CreateTableAPI(statement types.CreateTableStatement) error {
	err:= CatalogManager.CreateTableCheck(statement)
	if err!=nil {
		return err
	}
	return RecordManager.CreateTable(statement.TableName)
}

func CreateIndexAPI(statement types.CreateIndexStatement) error  {
	err,indexCatalog:=CatalogManager.CreateIndexCheck(statement)
	if err!=nil {
		return err
	}
	return RecordManager.CreateIndex(CatalogManager.GetTableCatalogUnsafe(statement.TableName),*indexCatalog)
}

func DropTableAPI(statement types.DropTableStatement) error  {
	err:=CatalogManager.DropTableCheck(statement)
	if err!=nil{
		return err
	}
	return RecordManager.DropTable(statement.TableName)
}

func DropIndexAPI(statement types.DropIndexStatement) error  {
	err:=CatalogManager.DropIndexCheck(statement)
	if err!=nil{
		return err
	}
	return RecordManager.DropIndex(CatalogManager.GetTableCatalogUnsafe(statement.TableName),statement.IndexName)
}


func InsertAPI(statement types.InsertStament) error  {
	err,colPos,startBytePos:= CatalogManager.InsertCheck(statement)
	if err!=nil{
		return err
	}
	err=RecordManager.InsertRecord(CatalogManager.GetTableCatalogUnsafe(statement.TableName),colPos,startBytePos,statement.Values)
	return err
}

func UpdateAPI(statement types.UpdateStament) error  {
	err,setColumns,values, exprLSRV:=CatalogManager.UpdateCheck(statement)
	if err!=nil{
		return err
	}
	var rowNum int
	if exprLSRV==nil{
		err,rowNum=RecordManager.UpdateRecord(CatalogManager.GetTableCatalogUnsafe(statement.TableName),setColumns,values,statement.Where)
	} else {
		err,rowNum=RecordManager.UpdateRecordWithIndex(CatalogManager.GetTableCatalogUnsafe(statement.TableName),setColumns,values,statement.Where,*exprLSRV)
	}
	if err!=nil {
		return err
	}
	fmt.Println(rowNum)
	return nil
}

func DeleteAPI(statement types.DeleteStatement) error {
	err,exprLSRV:=CatalogManager.DeleteCheck(statement)
	if err!=nil	{
		return err
	}
	var rowNum int
	if exprLSRV==nil{
		err,rowNum=RecordManager.DeleteRecord(CatalogManager.GetTableCatalogUnsafe(statement.TableName),statement.Where)
	} else {
		err,rowNum=RecordManager.DeleteRecordWithIndex(CatalogManager.GetTableCatalogUnsafe(statement.TableName),statement.Where,*exprLSRV)
	}
	fmt.Println(rowNum)
	return  nil
}

func SelectAPI(statement types.SelectStatement) error  {
	err,exprLSRV:=CatalogManager.SelectCheck(statement)
	if err!=nil {
		return err
	}
	var rowNum []value.Row
	if exprLSRV==nil{
		err,rowNum=RecordManager.SelectRecord(CatalogManager.GetTableCatalogUnsafe(statement.TableNames[0]),statement.Fields.ColumnNames,statement.Where)
	} else {
		err,rowNum=RecordManager.SelectRecordWithIndex(CatalogManager.GetTableCatalogUnsafe(statement.TableNames[0]),statement.Fields.ColumnNames,statement.Where,*exprLSRV)
	}
	if err!=nil {
		return err
	}
	for _,item:=range rowNum{
		b:=bytes.NewBuffer([]byte{})
		for _,v:=range item.Values {
			b.WriteString(v.String())
		}
		fmt.Println(b.String())
	}
	return nil
}