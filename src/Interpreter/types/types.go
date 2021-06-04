package types

import (
	"fmt"
	"minisql/src/Interpreter/value"
)

// NOTE aliases to refer from parser.
const (
	True  = true
	False = false
)

type OnDelete int
type KeyOrder int
type ScalarColumnTypeTag int
type OperationType int
const (
	NoAction OnDelete = iota
	Cascade
)

const (
	Asc KeyOrder = iota
	Desc
)

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
	CreateDatabase OperationType= iota
	CreateTable
	CreateIndex
	DropTable
	DropIndex
	DropDatabase
	Insert
	Update
	Delete
	Select
)

type DStatements interface {
	GetOperationType() OperationType
}
// DDStatements has parsed statements.
//type DDStatements struct {
//	CreateDatabases []CreateDatabaseStatement
//	CreateTables    []CreateTableStatement
//	CreateIndexes    []CreateIndexStatement
//	DropDatabses    []DropDatabaseStatement
//	DropTables      []DropTableStatement
//	DropIndexes     []DropIndexStatement
//}

// Column is a table column.


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

// Key is a table key.
type Key struct {
	Name     string
	KeyOrder KeyOrder
}

// Cluster is a Spanner table cluster.
type Cluster struct {
	TableName string
	OnDelete  OnDelete
}

// StoringClause is a storing clause info.
type StoringClause struct {
	ColumnNames []string
}

// Interleave is a interlive.
type Interleave struct {
	TableName string
}






// CreateDatabaseStatement is a 'CREATE DATABASE' statement info.
type CreateDatabaseStatement struct {
	DatabaseId string
}

func (c CreateDatabaseStatement)GetOperationType() OperationType {
	return CreateDatabase
}
// CreateTableStatement is a 'CREATE TABLE' statement info.
type CreateTableStatement struct {
	TableName   string
	Columns     []Column
	PrimaryKeys []Key
	Cluster     Cluster
}
func (c CreateTableStatement)GetOperationType() OperationType {
	return CreateTable
}

// CreateIndexStatement is a 'CREATE INDEX' statement info.
type CreateIndexStatement struct {
	IndexName     string
	Unique        bool
	TableName     string
	Keys          []Key
	StoringClause StoringClause
	Interleaves   []Interleave
}
func (c CreateIndexStatement)GetOperationType() OperationType {
	return CreateIndex
}


// DropDatabaseStatement is a 'DROP TABLE' statement info.
type DropDatabaseStatement struct {
	DatabaseId string
}
func (c DropDatabaseStatement)GetOperationType() OperationType {
	return DropDatabase
}


// DropTableStatement is a 'DROP TABLE' statement info.
type DropTableStatement struct {
	TableName string
}
func (c DropTableStatement)GetOperationType() OperationType {
	return DropTable
}


// DropIndexStatement is a 'DROP INDEX' statement info.
type DropIndexStatement struct {
	IndexName string
}
func (c DropIndexStatement)GetOperationType() OperationType {
	return DropIndex
}


// SelectStatement is a 'SELECT' statement info.
type SelectStatement struct {
	Fields FieldsName
	TableNames []string
	Where *Where  //maybe is nil!!!
	OrderBy []Order
	Limit Limit  //maybe is nil!!!
}

func (s SelectStatement) GetOperationType() OperationType {
	return Select
}

type (
	Where struct {
		Expr Expr
	}
	Expr interface {
		Evaluate(cols []string,row []value.Value)(bool, error)
		GetTargetCols()[]string
		Debug()
	}
	ComparisonExpr struct {
		Left string
		Operator value.CompareType
		Right value.Value
	}
	AndExpr struct {
		Left,Right Expr
	}
	OrExpr struct {
		Left,Right Expr
	}
	NotExpr struct {
		Expr Expr
	}
	Limit struct {
		Offset, Rowcount int
	}
	Order struct {
		Col string
		Direction KeyOrder
	}
	FieldsName struct {
		SelectAll bool
		ColumnNames	[]string
	}
	SetExpr struct {
		Left string
		Right value.Value
	}
)

func (e *ComparisonExpr)Evaluate(cols []string,row []value.Value)(bool,error)  {
	hit:=false
	idx:=0
	for i,col:=range cols{
		if col==e.Left{
			hit=true
			idx=i
			break
		}
	}
	if !hit {
		return true,nil
	}
	val := row[idx]
	if _,ok:=val.(value.Null);ok{
		if _, iok := e.Right.(value.Null); iok {
			if e.Operator==value.Equal {
				return true, nil
			}
			return false, nil
		}
	}
	if _, ok := e.Right.(value.Null); ok {
		if e.Operator == value.NotEqual {
			return true, nil
		}
		return false, nil
	}
	return e.Right.SafeCompare(val,e.Operator)
}
func (e *ComparisonExpr) GetTargetCols() []string {
	return []string{e.Left}
}
func (e *ComparisonExpr)Debug()  {
	fmt.Println(e.Left,e.Operator,e.Right.String())
}
func (e *AndExpr) Evaluate(cols []string, row []value.Value) (bool, error) {
	leftOk, err := e.Left.Evaluate(cols, row)
	if err != nil {
		return false, err
	}
	rightOk, err := e.Right.Evaluate(cols, row)
	if err != nil {
		return false, err
	}
	if leftOk && rightOk {
		return true, nil
	}
	return false, nil
}

func (e *AndExpr) GetTargetCols() []string {
	return append(e.Left.GetTargetCols(), e.Right.GetTargetCols()...)  //maybe with duplicate
}
func (e *AndExpr)Debug() {
	e.Left.Debug()
	fmt.Println(" and ")
	e.Right.Debug()
}

func (e *OrExpr) Evaluate(cols []string, row []value.Value) (bool, error) {
	leftOk, err := e.Left.Evaluate(cols, row)
	if err != nil {
		return false, err
	}
	if leftOk {
		return true, nil
	}
	rightOk, err := e.Right.Evaluate(cols, row)
	if err != nil {
		return false, err
	}
	return rightOk, nil
}
func (e *OrExpr) GetTargetCols() []string {
	return append(e.Left.GetTargetCols(), e.Right.GetTargetCols()...)
}
func (e *OrExpr)Debug() {
	e.Left.Debug()
	fmt.Println( " or " )
	e.Right.Debug()

}
func (e *NotExpr) Evaluate(cols []string, row []value.Value) (bool, error) {
	ok, err := e.Expr.Evaluate(cols, row)
	if err != nil {
		return false, err
	}
	return !ok, nil
}
func (e *NotExpr) GetTargetCols() []string {
	return e.Expr.GetTargetCols()
}
func (e *NotExpr)Debug() {
	e.Expr.Debug()
	fmt.Println("not ")
}


type InsertStament struct {
	TableName     string
	ColumnNames []string
	Values        []value.Value
}
func (c InsertStament)GetOperationType() OperationType {
	return Insert
}
type UpdateStament struct {
	TableName     string
	SetExpr []SetExpr
	Where *Where  //maybe is nil!!!
}
func (c UpdateStament)GetOperationType() OperationType {
	return Update
}

type DeleteStatement struct {
	TableName     string
	Where         *Where //maybe is nil!!!
}
func (c DeleteStatement)GetOperationType() OperationType {
	return Delete
}