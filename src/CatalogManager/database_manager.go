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
	TableCatalogMap map[string]*TableCatalog  //table name to catalog
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
		TableNum: 0,
	})
	fmt.Println(minisqlCatalog.Databases)
	return FlushDbMeta()
}

func UseDatabase(databaseId string) error  {
	if !ExistDatabase(databaseId) {
		return errors.New("database '"+databaseId+"' is not exist")
	}
	if databaseId==UsingDatabase.DatabaseId {
		return nil
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
		if err!=nil{
			return errors.New("Can't open " + databaseId + "'s index file")
		}
		rd:=msgp.NewReader(f)
		TableCatalogMap=make(map[string]*TableCatalog)
		for i:=0;i<UsingDatabase.TableNum;i++{
			tmpTableCatalog:=TableCatalog{}
			err=tmpTableCatalog.DecodeMsg(rd)
			if err!=nil {
				continue
			}
			fmt.Println(tmpTableCatalog)
			if len(tmpTableCatalog.TableName)>0 {
				TableCatalogMap[tmpTableCatalog.TableName]=&tmpTableCatalog
			}
		}
	}
	return nil
}
func DropDatabase(databaseId string) error  {
	filePos:=FolderPosition+DatabaseNamePrefix+databaseId
	for index,item:=range minisqlCatalog.Databases {
		if item.DatabaseId==databaseId {
			delete(TableCatalogMap, databaseId)
			minisqlCatalog.Databases=append(minisqlCatalog.Databases[:index],minisqlCatalog.Databases[index+1:]...)
			if Utils.Exists(filePos) {
			   _ =	Utils.RemoveFile(filePos)
			}
			return FlushDbMeta()
		}
	}
	if _,ok:=TableCatalogMap[databaseId];ok {
		return errors.New("Index data failed to synchronize correctly, please try to restart")
	}
	return errors.New("database '"+databaseId+"' is not exist")
}

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
		f,err=os.OpenFile(filePos,os.O_RDWR,0666)
		if err !=nil {
			return errors.New("open file meta of "+databaseId+ "fail")
		}
	}
	defer f.Close()
	wt:=msgp.NewWriter(f)

	for _,v:=range TableCatalogMap{
		err=v.EncodeMsg(wt)
		if err!=nil {
			return err
		}
	}
	return wt.Flush()

}