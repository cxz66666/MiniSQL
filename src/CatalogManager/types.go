package CatalogManager

import (
	"encoding/json"
	"minisql/src/Interpreter/types"
)

//go:generate msgp
type OnDelete=int
type KeyOrder=int
type ScalarColumnTypeTag=int
type OperationType=int

type TableCatalogMap map[string]*TableCatalog
const FolderPosition="./data/"
const MiniSqlCatalogName="minisql.meta"
const DatabaseNamePrefix="d_"
const (
	Bool ScalarColumnTypeTag = iota
	Int64
	Float64
	String
	Bytes
	Date
	Timestamp
	Null
	Alien
)
const (
	NoAction OnDelete = iota
	Cascade
)
const (
	Asc KeyOrder = iota
	Desc
)
type Column struct {
	Name    string
	Type    ColumnType
	Unique bool
	NotNull bool
	ColumnPos int   //the created position when table is created, this value is fixed
	StartBytesPos int //the start postion in record bytes array
}

type ColumnType struct {
	TypeTag ScalarColumnTypeTag
	Length  int
	IsArray bool
}
type Key struct {
	Name     string
	KeyOrder KeyOrder
}

// Cluster is a Spanner table cluster.
type Cluster struct {
	TableName string
	OnDelete  OnDelete
}
type TableCatalog struct {
	TableName   string
	ColumnsMap  map[string]Column
	PrimaryKeys []Key
	Cluster     Cluster
	Indexs      []IndexCatalog
	RecordCnt   int //RecordCnt means the now record number
	RecordTotal int //RecordTotal means the total number
	RecordLength int//RecordLength means a record length contains 3 parts, a vaild part , null bitmap, and record . use byte as the unit
}



// StoringClause is a storing clause info.
type StoringClause struct {
	ColumnNames []string
}

// Interleave is a interlive.
type Interleave struct {
	TableName string
}
type IndexCatalog struct {
	IndexName     string
	Unique        bool
	Keys          []Key
	StoringClause StoringClause
	Interleaves   []Interleave
}

type DatabaseCatalog struct {
	DatabaseId string
}

type MiniSqlCatalog struct {
	Databases    []DatabaseCatalog
}


func CreateTableStatement2TableCatalog(a *types.CreateTableStatement) *TableCatalog  {
	aj,_:=json.Marshal(&a)
	b:=new(TableCatalog)
	_=json.Unmarshal(aj,b)
	return b
}

func ColumnType2StringName(v ScalarColumnTypeTag) string {
	switch v {
	case Bool :
		return "BOOL"
	case Int64:
		return "INT64"
	case Float64:
		return "FLOAT64"
	case String:
		return "STRING"
	case Bytes:
		return "CHARS"
	case Date:
		return "DATE"
	case Timestamp:
		return "TIMESTAMP"
	case Null:
		return "NULL"
	case Alien:
		return "ALIEN"
	default:
		return "UNKNOW"
	}
	return "UNKNOW"
}