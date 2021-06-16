package API

import (
	"bytes"
	"errors"
	"fmt"
	"minisql/src/CatalogManager"
	"minisql/src/Interpreter/parser"
	"minisql/src/Interpreter/types"
	"minisql/src/Interpreter/value"
	"minisql/src/RecordManager"
	"minisql/src/Utils"
	"os"
	"sync"
)

func HandleOneParse( dataChannel <-chan types.DStatements,stopChannel chan<- struct{})   {
	var err error
	for statement:=range dataChannel {
		//fmt.Println(statement)
		switch statement.GetOperationType() {
		case types.CreateDatabase:
			err= CreateDatabaseAPI(statement.(types.CreateDatabaseStatement))
		case types.UseDatabase:
			err= UseDatabaseAPI(statement.(types.UseDatabaseStatement))
		case types.CreateTable:
			err= CreateTableAPI(statement.(types.CreateTableStatement))
		case types.CreateIndex:
			err= CreateIndexAPI(statement.(types.CreateIndexStatement))
		case types.DropTable:
			err= DropTableAPI(statement.(types.DropTableStatement))
		case types.DropIndex:
			err= DropIndexAPI(statement.(types.DropIndexStatement))
		case types.DropDatabase:
			err= DropDatabaseAPI(statement.(types.DropDatabaseStatement))
		case types.Insert:
			err= InsertAPI(statement.(types.InsertStament))
		case types.Update:
			err= UpdateAPI(statement.(types.UpdateStament))
		case types.Delete:
			err= DeleteAPI(statement.(types.DeleteStatement))
		case types.Select:
			err= SelectAPI(statement.(types.SelectStatement))
		case types.ExecFile:
			err=ExecFileAPI(statement.(types.ExecFileStatement))
		}
		//fmt.Println(err)
		stopChannel<- struct{}{}
	}
	fmt.Println(err)
	close(stopChannel)
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
	err,indexs:= CatalogManager.CreateTableCheck(statement)
	if err!=nil {
		return err
	}
	err=RecordManager.CreateTable(statement.TableName)
	if err!=nil{
		return err
	}
	for _,item:=range indexs{
		err=RecordManager.CreateIndex(CatalogManager.GetTableCatalogUnsafe(statement.TableName),item)
		if err!=nil{
			return err
		}
	}
	return nil
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

func ExecFileAPI(statement types.ExecFileStatement) error  {
	StatementChannel:=make(chan types.DStatements,100)
	FinishChannel:=make(chan struct{},100)
	if !Utils.Exists(statement.FileName) {
		return errors.New("file "+statement.FileName+" don't exist")
	}
	reader,err:=os.Open(statement.FileName)
	defer reader.Close()
	if err!=nil{
		return errors.New("open file "+statement.FileName+" fail")
	}
	var wg sync.WaitGroup
	wg.Add(1)

	go HandleOneParse(StatementChannel,FinishChannel)  //begin the runtime for exec
	go func() {
		defer wg.Done()
		for _=range FinishChannel {

		}
	}()
	err=parser.Parse(reader,StatementChannel)

	close(StatementChannel)

	wg.Wait()

	if err!=nil {
		return err
	}
	return nil
}