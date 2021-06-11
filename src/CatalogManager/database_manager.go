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
	TableName2CatalogMap TableCatalogMap  //table name to catalog
)
func ExistDatabase(databaseId string) bool   {
	for _,item:=range minisqlCatalog.Databases {
		if item.DatabaseId==databaseId {
			return true
		}
	}
	return false
}
func GetDatabaseCatalog(databaseId string) (DatabaseCatalog,bool) {
	for _,item:=range minisqlCatalog.Databases {
		if item.DatabaseId==databaseId {
			return  item,true
		}
	}
	return DatabaseCatalog{},false
}
func CreateDatabase(databaseId string) error {
	if ExistDatabase(databaseId) {
		return errors.New("database '"+databaseId+"' had been created")
	}
	filePos:=FolderPosition+DatabaseNamePrefix+databaseId
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
	filePos:=FolderPosition+DatabaseNamePrefix+databaseId
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

func DropDatabase(databaseId string) error  {
	if !ExistDatabase(databaseId) {
		return errors.New("Drop table "+databaseId+" doesn't exist")
	}
	if UsingDatabase.DatabaseId==databaseId {
		UsingDatabase=DatabaseCatalog{}
		TableName2CatalogMap=make(map[string]*TableCatalog)
	}
	filePos:=FolderPosition+DatabaseNamePrefix+databaseId
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
	filePos:=FolderPosition+DatabaseNamePrefix+databaseId
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