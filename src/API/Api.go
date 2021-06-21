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


//HandleOneParse 用来处理parse处理完的DStatement类型  dataChannel是接收Statement的通道,整个mysql运行过程中不会关闭，但是quit后就会关闭
//stopChannel 用来发送同步信号，每次处理完一个后就发送一个信号用来同步两协程，主协程需要接收到stopChannel的发送后才能继续下一条指令，当dataChannel
//关闭后，stopChannel才会关闭
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
		fmt.Println(err)
		stopChannel<- struct{}{}
	}
	fmt.Println(err)
	close(stopChannel)
}
//CreateDatabaseAPI 只调用CM，和IM、RM无关
func CreateDatabaseAPI(statement types.CreateDatabaseStatement)  error {
	return CatalogManager.CreateDatabase(statement.DatabaseId)
}
//UseDatabaseAPI 只调用CM，和IM、RM无关
func UseDatabaseAPI(statement types.UseDatabaseStatement) error  {
	return CatalogManager.UseDatabase(statement.DatabaseId)
}
//DropDatabaseAPI  先CM的check，和IM、RM无关 ，再调用RM的drop ， 再在CM中删除并flush
func DropDatabaseAPI(statement types.DropDatabaseStatement) error  {
	err:= CatalogManager.DropDatabaseCheck(statement.DatabaseId)
	if err!=nil {
		return err
	}
	err=RecordManager.DropDatabase(statement.DatabaseId)
	if err!=nil {
		return err
	}
	return CatalogManager.DropDatabase(statement.DatabaseId)
}

//CreateTableAPI CM进行检查，index检查 语法检查  之后调用RM中的CreateTable创建表， 之后使用RM中的CreateIndex建索引
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
	err=CatalogManager.FlushDatabaseMeta(CatalogManager.UsingDatabase.DatabaseId)
	return err
}

//CreateIndexAPI CM进行检查，index语法检查 之后使用RM中的CreateIndex建索引
func CreateIndexAPI(statement types.CreateIndexStatement) error  {
	err,indexCatalog:=CatalogManager.CreateIndexCheck(statement)
	if err!=nil {
		return err
	}
	return RecordManager.CreateIndex(CatalogManager.GetTableCatalogUnsafe(statement.TableName),*indexCatalog)
}

//DropTableAPI CM进行检查，注意这个时候并不真正删除CM中的记录， 之后RM的DropTable删除table文件以及index文件， 之后让CM删除map中的记录同时flush
func DropTableAPI(statement types.DropTableStatement) error  {
	err:=CatalogManager.DropTableCheck(statement)
	if err!=nil{
		return err
	}
	err= RecordManager.DropTable(statement.TableName)
	if err!=nil {
		return err
	}
	return CatalogManager.DropTable(statement)
}
//DropIndexAPI CM进行检查， RM中删除index 之后CM中再删除并flush
func DropIndexAPI(statement types.DropIndexStatement) error  {
	err:=CatalogManager.DropIndexCheck(statement)
	if err!=nil{
		return err
	}
	err= RecordManager.DropIndex(CatalogManager.GetTableCatalogUnsafe(statement.TableName),statement.IndexName)
	if err!=nil {
		return err
	}
	return CatalogManager.DropIndex(statement)
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
// ExecFileAPI 执行某文件  开辟两个新协程
func ExecFileAPI(statement types.ExecFileStatement) error  {
	//parse协程 有缓冲信道
	StatementChannel:=make(chan types.DStatements,500)
	FinishChannel:=make(chan struct{},500)
	if !Utils.Exists(statement.FileName) {
		return errors.New("file "+statement.FileName+" don't exist")
	}
	reader,err:=os.Open(statement.FileName)
	defer reader.Close()
	if err!=nil{
		return errors.New("open file "+statement.FileName+" fail")
	}
	var wg sync.WaitGroup
	wg.Add(1)  //等待FinishChannel关闭

	go HandleOneParse(StatementChannel,FinishChannel)  //begin the runtime for exec
	go func() {
		defer wg.Done()
		for _=range FinishChannel {

		}
	}()
	err=parser.Parse(reader,StatementChannel) //开始解析

	close(StatementChannel) //关闭StatementChannel，进而关闭FinishChannel

	wg.Wait()

	if err!=nil {
		return err
	}
	return nil
}