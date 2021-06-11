package CatalogManager

import (
	"errors"
	"minisql/src/Interpreter/types"
)
const prefix_primarykey="primary_key"
func createTableInitAndCheck(statement *TableCatalog) error  {
	recordlength:=0
	columnNum:=0
	bytesPos:=make([]int,len(statement.ColumnsMap)+1)

	for _,item:=range statement.ColumnsMap{ //check the type and length
		if item.Type.TypeTag>Timestamp || item.Type.TypeTag<Bool {
			return errors.New("column "+item.Name+" has a illegal type")
		}
		if item.Type.TypeTag==Bytes &&item.Type.Length>255 {
			return errors.New("column "+item.Name+" has a length > 255, please set the length between 0~255")
		}
		switch item.Type.TypeTag {
		case Bool:
			recordlength+=1
			bytesPos[item.ColumnPos]=1
		case Int64:
			recordlength+=8
			bytesPos[item.ColumnPos]=8
		case Float64:
			recordlength+=8
			bytesPos[item.ColumnPos]=8
		case String,Bytes:
			recordlength+=item.Type.Length   //string is not like thess, but nowsday we don't use string type
			bytesPos[item.ColumnPos]=item.Type.Length
		case Date:
			recordlength+=5  //I don't know how length
			bytesPos[item.ColumnPos]=5
		case Timestamp:
			recordlength+=8 //I don't know
			bytesPos[item.ColumnPos]=8
		case Null:
			recordlength+=8 //it can't be null at create time
			bytesPos[item.ColumnPos]=8
		case Alien:
			recordlength+=0  // I don't know
			bytesPos[item.ColumnPos]=0
		}
		columnNum+=1
	}
	recordlength+=(columnNum)/8+1  //bit map and a vaild part!!
	bytesPos[0]+=(columnNum)/8+1
	for i:=1;i<len(statement.ColumnsMap);i++{
		bytesPos[i]+=bytesPos[i-1]
	}
	for k,v:=range statement.ColumnsMap {
		v.StartBytesPos=bytesPos[v.ColumnPos]
		statement.ColumnsMap[k]=v
	}
	//keys:=make([]Key,0,6)//this key maybe a composite keys, so it's needed to store the keys and names
	//var indexName string
	//for _,item:=range statement.PrimaryKeys { //key name must exist in Columns name
	//	if _,ok:=statement.ColumnsMap[item.Name];!ok {
	//		return errors.New("primary key error, don't have a column name "+item.Name)
	//	}
	//
	//	keys=append(keys,Key{    //add the key to the keys
	//		Name: item.Name,
	//		KeyOrder: item.KeyOrder,
	//	})
	//	indexName=indexName+"_"+item.Name
	//}

	//create a default index, use primary key (maybe composite keys!!)

	//newIndex:=IndexCatalog{
	//	IndexName: prefix_primarykey+indexName,
	//	Unique: true,
	//	Keys: keys,
	//	StoringClause: StoringClause{},//now we don't use it, but we also store it
	//	Interleaves: []Interleave{}, //keep empty
	//}
	//statement.Indexs= append(statement.Indexs,newIndex )
	statement.RecordLength=recordlength
	return nil
}

func CreateTable(statement types.CreateTableStatement) error  {
	if len(UsingDatabase.DatabaseId) ==0 {
		return errors.New("Don't use database, please create table after using database")
	}
	if _,ok:=TableName2CatalogMap[statement.TableName];ok {
		return errors.New("Table "+statement.TableName+" already exists")
	}
	newCatalog:=CreateTableStatement2TableCatalog(&statement)
	err:=createTableInitAndCheck(newCatalog)
	if err != nil{
		return err
	}
	if newCatalog!=nil {
		TableName2CatalogMap[statement.TableName]=newCatalog

	} else {
		return errors.New("fail to conver type, internal errors")
	}

	_= AddTableToCatalog(UsingDatabase.DatabaseId)
	return FlushDatabaseMeta(UsingDatabase.DatabaseId)
}
//DropTableCheck don't delete the map[id] and the catalog file, just check the legal
func DropTableCheck(statement types.DropTableStatement) error{
	if len(UsingDatabase.DatabaseId)==0 {
		return errors.New("Don't use database, please drop table after using database")
	}
	if _,ok:=TableName2CatalogMap[statement.TableName]; !ok {
		return errors.New("Table "+statement.TableName+" doesn't exists")
	}
	return nil
}
func DropTable(statement types.DropTableStatement) error  {
	err:=DropTableCheck(statement)
	if err!=nil {
		return err
	}
	delete(TableName2CatalogMap,statement.TableName)
	_=DeleteTableToCatalog(UsingDatabase.DatabaseId)
	return 	FlushDatabaseMeta(UsingDatabase.DatabaseId)
}

func  AddTableToCatalog(databaseId string) error  {
	for _,item:=range minisqlCatalog.Databases {
		if item.DatabaseId==databaseId {
				return nil
		}
	}
	return errors.New("not found database")
}

func DeleteTableToCatalog(databaseId string) error  {
	for _,item:=range minisqlCatalog.Databases {
		if item.DatabaseId==databaseId {
			return nil
		}
	}
	return errors.New("not found database")
}