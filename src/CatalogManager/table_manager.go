package CatalogManager

import (
	"errors"
	"minisql/src/Interpreter/types"
)
const prefix_primarykey="primary_key"
//createTableInitAndCheck createtable前的检查
func createTableInitAndCheck(statement *TableCatalog) (error,[]IndexCatalog) {
	recordlength:=0
	columnNum:=0
	bytesPos:=make([]int,len(statement.ColumnsMap)+1)

	for _,item:=range statement.ColumnsMap{ //check the type and length
		if item.Type.TypeTag>Timestamp || item.Type.TypeTag<Bool {
			return errors.New("column "+item.Name+" has a illegal type"),nil
		}
		if item.Type.TypeTag==Bytes &&item.Type.Length>255 {
			return errors.New("column "+item.Name+" has a length > 255, please set the length between 0~255"),nil
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
	toolBytes:=(columnNum)/8+1
	recordlength+= toolBytes //bit map and a vaild part!!


	for i:=0;i<len(statement.ColumnsMap);i++{
		tmpNum:=bytesPos[i]
		bytesPos[i]=toolBytes
		toolBytes+=tmpNum
	}
	//奇怪的算法，先从1-n-1累加，然后将第0位置为初始值
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
	if len(statement.PrimaryKeys) > 0 {
		keyname:=statement.PrimaryKeys[0].Name
		if item,ok:=statement.ColumnsMap[keyname];!ok{
			return errors.New("primary key error, don't have a column name "+item.Name),nil
		} else {
			item.Unique=true
			item.NotNull=true
			statement.ColumnsMap[keyname]=item
		}
	}

	indexs:=make([]IndexCatalog,0)
	for _,item:=range statement.ColumnsMap{
		if item.Unique{
			indexs=append(indexs,IndexCatalog{
				IndexName: item.Name+"_index",
				Unique: true,
				Keys: []Key{
					{
						Name: item.Name,
						KeyOrder: Asc,
					},
				},
			})
		}
	}
	return nil,indexs
}
//CreateTableCheck 用来检查table，并返回所有的应该建的索引
func CreateTableCheck(statement types.CreateTableStatement) (error,[]IndexCatalog)  {
	if len(UsingDatabase.DatabaseId) ==0 {
		return errors.New("Don't use database, please create table after using database"),nil
	}
	if _,ok:=TableName2CatalogMap[statement.TableName];ok {
		return errors.New("Table "+statement.TableName+" already exists"),nil
	}
	newCatalog:=CreateTableStatement2TableCatalog(&statement)
	err,indexs:=createTableInitAndCheck(newCatalog)
	if err != nil{
		return err,nil
	}
	if newCatalog!=nil {
		TableName2CatalogMap[statement.TableName]=newCatalog

	} else {
		return errors.New("fail to conver type, internal errors"),nil
	}

	//_= AddTableToCatalog(UsingDatabase.DatabaseId)
	return FlushDatabaseMeta(UsingDatabase.DatabaseId),indexs
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
//DropTable 真正删除table文件与catalog
func DropTable(statement types.DropTableStatement) error  {
	err:=DropTableCheck(statement)
	if err!=nil {
		return err
	}
	delete(TableName2CatalogMap,statement.TableName)
	//_=DeleteTableToCatalog(UsingDatabase.DatabaseId)
	return 	FlushDatabaseMeta(UsingDatabase.DatabaseId)
}
//
//func  AddTableToCatalog(databaseId string) error  {
//	for _,item:=range minisqlCatalog.Databases {
//		if item.DatabaseId==databaseId {
//				return nil
//		}
//	}
//	return errors.New("not found database")
//}
//
//func DeleteTableToCatalog(databaseId string) error  {
//	for _,item:=range minisqlCatalog.Databases {
//		if item.DatabaseId==databaseId {
//			return nil
//		}
//	}
//	return errors.New("not found database")
//}

//GetTableCatalogUnsafe 不安全的拿到table的catalog
func GetTableCatalogUnsafe(tableName string) *TableCatalog  {
	return TableName2CatalogMap[tableName]
}
//GetTableColumnsInOrder 顺序返回该表的列名（按照建表时候的顺序）
func GetTableColumnsInOrder(table string) []string  {
	tablecm:=TableName2CatalogMap[table]
	ans:=make([]string,len(tablecm.ColumnsMap))
	for _,item:=range tablecm.ColumnsMap {
		ans[item.ColumnPos]=item.Name
	}
	return ans
}