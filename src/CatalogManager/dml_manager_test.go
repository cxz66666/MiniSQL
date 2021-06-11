package CatalogManager

import (
	"fmt"
	"minisql/src/Interpreter/parser"
	"minisql/src/Interpreter/types"
	"strings"
	"testing"
)
var insertStatement=[]string{
	"insert into cxz(column1,column2) values (123,'12');",
	"insert into cxz  values (123,'123',234.234,'abcdefghighklf');",
}
var deleteStatement=[]string{
	"delete from cxz where column1=column1 and column2=324 and column3='234';",
	"delete from cxzwe where column2='34512' or 1=1;",
	"delete from syf;",
}
var updateStatement=[]string{
	"update cxz set column1="
}
//please use TestCreateTable to create table before use TestInsertCheck
func TestInsertCheck(t *testing.T) {
	LoadDbMeta()
	fmt.Println(CreateDatabase("123123"))
	fmt.Println(CreateDatabase("4564546"))
	fmt.Println(UseDatabase("123123"))
	for k,v:=range  TableName2CatalogMap{
		fmt.Println(k,*v)
	}
	for _,statement:=range insertStatement {
	 inserts,_:=parser.Parse(strings.NewReader(statement))
	 fmt.Println(*inserts)
	 fmt.Println(InsertCheck((*inserts)[0].(types.InsertStament)))
	}
}

func BenchmarkInsertCheck(b *testing.B) {
	LoadDbMeta()
	fmt.Println(CreateDatabase("123123"))
	fmt.Println(CreateDatabase("4564546"))
	fmt.Println(UseDatabase("123123"))
	b.Run("simple insert check", func(b *testing.B) {
		for i:=0;i<b.N;i++{
			inserts,_:=parser.Parse(strings.NewReader(insertStatement[0]))
			fmt.Println(InsertCheck((*inserts)[0].(types.InsertStament)))
		}
	})

}
//please use TestCreateTable to create table before use TestDeleteCheck
func TestDeleteCheck(t *testing.T) {
	LoadDbMeta()
	fmt.Println(CreateDatabase("123123"))
	fmt.Println(CreateDatabase("4564546"))
	fmt.Println(UseDatabase("123123"))
	for k,v:=range  TableName2CatalogMap{
		fmt.Println(k,*v)
	}
	for _,statement:=range deleteStatement {
		deletes,_:=parser.Parse(strings.NewReader(statement))
		fmt.Println(*deletes)
		fmt.Println(DeleteCheck((*deletes)[0].(types.DeleteStatement)))
	}
}

func BenchmarkDeleteCheck(b *testing.B) {
	LoadDbMeta()
	fmt.Println(CreateDatabase("123123"))
	fmt.Println(CreateDatabase("4564546"))
	fmt.Println(UseDatabase("123123"))
	b.Run("simple delete check", func(b *testing.B) {
		for i:=0;i<b.N;i++{
			for _,statement:=range deleteStatement {
				deletes,_:=parser.Parse(strings.NewReader(statement))
				fmt.Println(*deletes)
				fmt.Println(DeleteCheck((*deletes)[0].(types.DeleteStatement)))
			}
		}
	})
}
