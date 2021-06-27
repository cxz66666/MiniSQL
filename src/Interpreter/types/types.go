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
//OnDelete is used for on delete behave
type OnDelete=int
const (
	NoAction OnDelete = iota
	Cascade
)
//KeyOrder order for key
type KeyOrder=int
const (
	Asc KeyOrder = iota
	Desc
)

//ScalarColumnTypeTag is the type
type ScalarColumnTypeTag=int
const (
	Bool ScalarColumnTypeTag = iota
	Int64
	Float64
	String
	Bytes
	Date
	Timestamp
)

type OperationType=int
const (
	CreateDatabase OperationType= iota
	UseDatabase
	CreateTable
	CreateIndex
	DropTable
	DropIndex
	DropDatabase
	Insert
	Update
	Delete
	Select
	ExecFile
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
	ColumnPos int   //the created position when table is created, this value is fixed
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

// UseDatabaseStatement is a 'Use DATABASE' statement info.
type UseDatabaseStatement struct {
	DatabaseId string
}

func (c UseDatabaseStatement)GetOperationType() OperationType {
	return UseDatabase
}

// CreateTableStatement is a 'CREATE TABLE' statement info.
type CreateTableStatement struct {
	TableName   string
	ColumnsMap    map[string]Column
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
	TableName string
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

type ExecFileStatement struct {
	FileName string
}

func (s ExecFileStatement)GetOperationType() OperationType  {
	return ExecFile
}
type (
	//Where is the type for where func which maybe nil!
	Where struct {
		Expr Expr
	}
	Expr interface {
		Evaluate(row []value.Value)(bool, error)
		GetTargetCols()[]string
		Debug()
		GetTargetColsNum()int
		//GetIndexExpr input a index column name, and find whether have a name same as index
		GetIndexExpr(string) (bool,*ComparisonExprLSRV)
	}
	//ComparisonExprLSRV left string right value
	ComparisonExprLSRV struct {
		Left string
		Operator value.CompareType
		Right value.Value
	}
	ComparisonExprLVRS struct {
		Left value.Value
		Operator value.CompareType
		Right string
	}
	ComparisonExprLVRV struct {
		Left value.Value
		Operator value.CompareType
		Right value.Value
	}
	ComparisonExprLSRS struct {
		Left string
		Operator value.CompareType
		Right string
	}
	AndExpr struct {
		Left,Right Expr
		LeftNum,RightNum int
	}
	OrExpr struct {
		Left,Right Expr
		LeftNum,RightNum int
	}
	NotExpr struct {
		Expr Expr
		LeftNum int
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

func (e *ComparisonExprLSRV)Evaluate(row []value.Value)(bool,error)  {
	val := row[0]
	if _,ok:=val.(value.Null);ok{ //left string's value is NULL
		if _, iok := e.Right.(value.Null); iok {  //right is also NULL
			if e.Operator==value.Equal {
				return true, nil
			}
			return false, nil
		} else {
			if e.Operator==value.NotEqual {
				return true,nil
			}
			return false,nil
		}
	}
	if _, ok := e.Right.(value.Null); ok { //left not NULL
		if e.Operator == value.NotEqual {
			return true, nil
		}
		return false, nil
	}
	return val.SafeCompare(e.Right,e.Operator)
}
func (e *ComparisonExprLSRV) GetTargetCols() []string {
	return []string{e.Left}
}
func (e *ComparisonExprLSRV) GetTargetColsNum() int {
	return 1
}
func (e *ComparisonExprLSRV)Debug()  {
	fmt.Println(e.Left,e.Operator,e.Right.String())
}
func (e *ComparisonExprLSRV)GetIndexExpr(indexName string) (bool,*ComparisonExprLSRV){
	if e.Left==indexName&&e.Operator!=value.NotEqual{
		return true,&ComparisonExprLSRV{Left: e.Left,Operator: e.Operator,Right: e.Right}
	}
	return false,nil
}

func (e *ComparisonExprLVRS)Evaluate(row []value.Value)(bool,error)  {
	val := row[0]
	if _,ok:=val.(value.Null);ok{
		if _, iok := e.Left.(value.Null); iok {
			if e.Operator==value.Equal {
				return true, nil
			}
			return false, nil
		} else {
			if e.Operator==value.NotEqual {
				return true,nil
			}
			return false,nil
		}
	}
	if _, ok := e.Left.(value.Null); ok {
		if e.Operator == value.NotEqual {
			return true, nil
		}
		return false, nil
	}
	return e.Left.SafeCompare(val,e.Operator)
}
func (e *ComparisonExprLVRS) GetTargetCols() []string {
	return []string{e.Right}
}
func (e *ComparisonExprLVRS) GetTargetColsNum() int {
	return 1
}
func (e *ComparisonExprLVRS)Debug()  {
	fmt.Println(e.Left.String(),e.Operator,e.Right)
}
func (e *ComparisonExprLVRS)GetIndexExpr(indexName string) (bool,*ComparisonExprLSRV){
	if e.Right==indexName&&e.Operator!=value.NotEqual{
		return true,&ComparisonExprLSRV{Left: e.Right,Operator: e.Operator,Right: e.Left}
	}
	return false,nil
}


func (e *ComparisonExprLVRV)Evaluate(row []value.Value)(bool,error)  {
	return e.Left.SafeCompare(e.Right,e.Operator)
}
func (e *ComparisonExprLVRV) GetTargetCols() []string {
	return []string{}
}
func (e *ComparisonExprLVRV) GetTargetColsNum() int {
	return 0
}
func (e *ComparisonExprLVRV)Debug()  {
	fmt.Println(e.Left.String(),e.Operator,e.Right.String())
}
func (e *ComparisonExprLVRV)GetIndexExpr(indexName string) (bool,*ComparisonExprLSRV){
	return false,nil
}

func (e *ComparisonExprLSRS)Evaluate(row []value.Value)(bool,error)  {
	vall := row[0]
	valr := row[1]
	if _,ok:=vall.(value.Null);ok{  //left is NULL
		if _, iok := valr.(value.Null); iok { //right is also NULL
			if e.Operator==value.Equal {
				return true, nil
			}  //
			return false, nil
		}  else {
			if e.Operator==value.NotEqual {
				return true,nil
			}
			return false,nil
		}
	}
	if _, ok := valr.(value.Null); ok {
		if e.Operator == value.NotEqual {
			return true, nil
		}
		return false, nil
	}
	return vall.SafeCompare(valr,e.Operator)
}
func (e *ComparisonExprLSRS) GetTargetCols() []string {
	return []string{e.Left,e.Right}
}
func (e *ComparisonExprLSRS) GetTargetColsNum() int {
	return 2
}
func (e *ComparisonExprLSRS)Debug()  {
	fmt.Println(e.Left,e.Operator,e.Right)
}
func (e *ComparisonExprLSRS)GetIndexExpr(indexName string) (bool,*ComparisonExprLSRV){
	return false,nil
}




func (e *AndExpr) Evaluate(row []value.Value) (bool, error) {
	leftOk, err := e.Left.Evaluate(row[0:e.LeftNum])
	if err != nil {
		return false, err
	}
	rightOk, err := e.Right.Evaluate(row[e.LeftNum:e.LeftNum+e.RightNum])
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
func (e *AndExpr) GetTargetColsNum() int {
	return e.LeftNum+e.RightNum
}
func (e *AndExpr)Debug() {
	e.Left.Debug()
	fmt.Println(" and ")
	e.Right.Debug()
}
func (e *AndExpr)GetIndexExpr(indexName string) (bool,*ComparisonExprLSRV){
	b,c:=e.Left.GetIndexExpr(indexName)
	if b==true {
		b1,c1:=e.Right.GetIndexExpr(indexName)
		if b1==true&&c1!=nil&&c1.Operator==value.Equal {
			return true,c1
		}
		return b,c
	}
	return e.Right.GetIndexExpr(indexName)
}

func (e *OrExpr) Evaluate(row []value.Value) (bool, error) {
	leftOk, err := e.Left.Evaluate(row[0:e.LeftNum])
	if err != nil {
		return false, err
	}
	if leftOk {
		return true, nil
	}
	rightOk, err := e.Right.Evaluate(row[e.LeftNum:e.LeftNum+e.RightNum])
	if err != nil {
		return false, err
	}
	return rightOk, nil
}
func (e *OrExpr) GetTargetCols() []string {
	return append(e.Left.GetTargetCols(), e.Right.GetTargetCols()...)
}
func (e *OrExpr) GetTargetColsNum() int {
	return e.LeftNum+e.RightNum
}
func (e *OrExpr)Debug() {
	e.Left.Debug()
	fmt.Println( " or " )
	e.Right.Debug()

}
//GetIndexExpr 注意 如果是or表达式 直接返回false，因此没法走单索引
func (e *OrExpr)GetIndexExpr(indexName string) (bool,*ComparisonExprLSRV){
	return false,nil
}

func (e *NotExpr) Evaluate(row []value.Value) (bool, error) {
	ok, err := e.Expr.Evaluate(row)
	if err != nil {
		return false, err
	}
	return !ok, nil
}
func (e *NotExpr) GetTargetCols() []string {
	return e.Expr.GetTargetCols()
}
func (e *NotExpr) GetTargetColsNum() int {
	return e.LeftNum
}
func (e *NotExpr)Debug() {
	e.Expr.Debug()
	fmt.Println("not ")
}
func (e *NotExpr)GetIndexExpr(indexName string) (bool,*ComparisonExprLSRV){
	return e.Expr.GetIndexExpr(indexName)
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