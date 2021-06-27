package CatalogManager

import (
	"errors"
	"fmt"
	"github.com/tinylib/msgp/msgp"
	"minisql/src/Utils"
	"os"
)
var(
	UsingDatabase DatabaseCatalog
	TableName2CatalogMap TableCatalogMap=make(map[string]*TableCatalog)  //table name to catalog
)
//ExistDatabase 用来判断该数据库是否存在
func ExistDatabase(databaseId string) bool   {
	for _,item:=range minisqlCatalog.Databases {
		if item.DatabaseId==databaseId {
			return true
		}
	}
	return false
}
//GetDatabaseCatalog 用来获取该数据库的catalog
func GetDatabaseCatalog(databaseId string) (DatabaseCatalog,bool) {
	for _,item:=range minisqlCatalog.Databases {
		if item.DatabaseId==databaseId {
			return  item,true
		}
	}
	return DatabaseCatalog{},false
}
//CreateDatabase 创建新的数据库
func CreateDatabase(databaseId string) error {
	if ExistDatabase(databaseId) {
		return errors.New("database '"+databaseId+"' had been created")
	}
	filePos:=DBCatalogPrefix()+databaseId
	if !Utils.Exists(filePos) {
		f, err := Utils.CreateFile(filePos)
		defer f.Close()
		if err != nil {
			return errors.New("Can't create " + databaseId + "'s index file")
		}
	} else {
		err:=Utils.RemoveFile(filePos)
		if err!=nil {
			return errors.New("Can't delete "+databaseId+"'s index file")
		}
		f, err := Utils.CreateFile(filePos)
		defer f.Close()
	}
	minisqlCatalog.Databases=append(minisqlCatalog.Databases,DatabaseCatalog{
		DatabaseId: databaseId,
	})
	//fmt.Println(minisqlCatalog.Databases)
	return FlushDbMeta()

}
//GetDBTablesMap 获取某数据库下的所有table信息，返回值为 TableCatalogMap
func GetDBTablesMap(databaseId string)  (TableCatalogMap,error) {
	if !ExistDatabase(databaseId) {
		return nil,errors.New("database '"+databaseId+"' is not exist")
	}
	filePos:=DBCatalogPrefix()+databaseId
	res:=make(TableCatalogMap)
	f,err:=os.Open(filePos)
	defer f.Close()
	if err!=nil{
		return nil,err
	}
	rd:=msgp.NewReader(f)
	err=res.DecodeMsg(rd)
	if err!=nil {
		if _,ok:=err.(msgp.Error);ok{
			return res,nil
		}
		return  nil,err
	}
	return res,nil
}
//UseDatabase 使用某个数据库，加载其文件catalog
func UseDatabase(databaseId string) error  {
	if !ExistDatabase(databaseId) {
		return errors.New("database '"+databaseId+"' is not exist")
	}
	if databaseId==UsingDatabase.DatabaseId {
		return nil
	}

	if len(UsingDatabase.DatabaseId)>0 { //write the last database index back
		FlushDatabaseMeta(UsingDatabase.DatabaseId)
	}
	filePos:=DBCatalogPrefix()+databaseId
	UsingDatabase,_=GetDatabaseCatalog(databaseId)

	if !Utils.Exists(filePos) {  //create the database index file
		f, err := Utils.CreateFile(filePos)
		defer f.Close()
		if err != nil {
			return errors.New("Can't create " + databaseId + "'s index file")
		}
	} else {
		f,err:=os.Open(filePos)
		defer f.Close()
		if err!=nil{
			return errors.New("Can't open " + databaseId + "'s index file")
		}
		rd:=msgp.NewReader(f)
		TableName2CatalogMap=make(map[string]*TableCatalog)
		err=TableName2CatalogMap.DecodeMsg(rd)
		if err!=nil {
			if _,ok:=err.(msgp.Error);ok{
				return  nil
			}
			return  err
		}
	}
	return nil
}
//DropDatabaseCheck 删除某database前的检查
func DropDatabaseCheck(databaseId string)error  {
	if !ExistDatabase(databaseId) {
		return errors.New("Drop table "+databaseId+" doesn't exist")
	}
	return nil
}
//DropDatabase 直接删除某数据库的文件
func DropDatabase(databaseId string) error  {
	if !ExistDatabase(databaseId) {
		return errors.New("Drop table "+databaseId+" doesn't exist")
	}
	if UsingDatabase.DatabaseId==databaseId {
		UsingDatabase=DatabaseCatalog{}
		TableName2CatalogMap=make(map[string]*TableCatalog)
	}
	filePos:=DBCatalogPrefix()+databaseId
	for index,item:=range minisqlCatalog.Databases {
		if item.DatabaseId==databaseId {
			minisqlCatalog.Databases=append(minisqlCatalog.Databases[:index],minisqlCatalog.Databases[index+1:]...)
			fmt.Println(minisqlCatalog.Databases)
			if Utils.Exists(filePos) {
				fmt.Println(filePos)
			   _ =	Utils.RemoveFile(filePos)
			}
			return FlushDbMeta()
		}
	}
	return errors.New("database '"+databaseId+"' is not exist")
}
//FlushDatabaseMeta will write the TableName2CatalogMap datas to storage
func FlushDatabaseMeta(databaseId string) error  {

	var f *os.File
	var err error
	filePos:=DBCatalogPrefix()+databaseId
	if !Utils.Exists(filePos) {
		f,err=Utils.CreateFile(filePos)
		if err !=nil {
			return errors.New("create file meta of "+databaseId+ "fail")
		}
	} else {
		f,err=os.OpenFile(filePos,os.O_WRONLY|os.O_TRUNC,0666)
		if err !=nil {
			return errors.New("open file meta of "+databaseId+ "fail")
		}
	}
	defer f.Close()
	wt:=msgp.NewWriter(f)
	err=TableName2CatalogMap.EncodeMsg(wt)
	if err!=nil {
		return err
	}
	return wt.Flush()

}