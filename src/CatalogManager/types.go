package CatalogManager

//go:generate msgp
type OnDelete int
type KeyOrder int
type ScalarColumnTypeTag int
type OperationType int

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
	Columns     []Column
	PrimaryKeys []Key
	Cluster     Cluster
	Indexs      []IndexCatalog
	recordCnt   int //recordCnt means the now record number
	recordTotal int //recordTotal means the total number
	recordLength int// a record length contains 3 parts, headers,record and tail ptr
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
	TableName     string
	Keys          []Key
	StoringClause StoringClause
	Interleaves   []Interleave
}

type DatabaseCatalog struct {
	DatabaseId string
	TableNum   int
}

type MiniSqlCatalog struct {
	Databases    []DatabaseCatalog
}