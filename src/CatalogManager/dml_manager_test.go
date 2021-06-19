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
	"insert into cxz values (123,'123',234.234,'abcdefghighklf','fdsgdfgerdterwtfsdf');",
}
var deleteStatement=[]string{
	"delete from cxz where column1=column1 and column2=324 and column3='234';",
	"delete from cxzwe where column2='34512' or 1=1;",
	"delete from syf;",
}
var updateStatement=[]string{
	"update cxz set column1=",
}
var (
	statementChannel chan types.DStatements
	finishChannel chan struct{}
)
//please use TestCreateTable to create table before use TestInsertCheck
func TestInsertCheck(t *testing.T) {
	LoadDbMeta()
	fmt.Println(CreateDatabase("123123"))
	fmt.Println(CreateDatabase("4564546"))
	fmt.Println(UseDatabase("123123"))
	for k,v:=range  TableName2CatalogMap{
		fmt.Println(k,*v)
	}
	statementChannel=make(chan types.DStatements,100)
	finishChannel=make(chan struct{},100)
	go func() {
		for item:=range statementChannel {
			fmt.Println(InsertCheck(item.(types.InsertStament)))
			finishChannel<- struct{}{}
		}
	}()
	for _,statement:=range insertStatement {
	 err:=parser.Parse(strings.NewReader(statement),statementChannel)
	 fmt.Println(err)
	 <-finishChannel
	}
}

func BenchmarkInsertCheck(b *testing.B) {
	LoadDbMeta()
	fmt.Println(CreateDatabase("123123"))
	fmt.Println(CreateDatabase("4564546"))
	fmt.Println(UseDatabase("123123"))
	statementChannel=make(chan types.DStatements,100)
	finishChannel=make(chan struct{},100)
	go func() {
		for item:=range statementChannel {
			fmt.Println(InsertCheck(item.(types.InsertStament)))
			finishChannel<- struct{}{}
		}
	}()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err:=parser.Parse(strings.NewReader(insertStatement[0]),statementChannel)
			fmt.Println(err)
			<-finishChannel
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
	statementChannel=make(chan types.DStatements,100)
	finishChannel=make(chan struct{},100)
	go func() {
		for item:=range statementChannel {
			fmt.Println(DeleteCheck(item.(types.DeleteStatement)))
			finishChannel<- struct{}{}
		}
	}()
	for _,statement:=range deleteStatement {
		err:=parser.Parse(strings.NewReader(statement),statementChannel)
		fmt.Println(err)
		<-finishChannel
	}
}

func BenchmarkDeleteCheck(b *testing.B) {
	LoadDbMeta()
	fmt.Println(CreateDatabase("123123"))
	fmt.Println(CreateDatabase("4564546"))
	fmt.Println(UseDatabase("123123"))
	statementChannel=make(chan types.DStatements,100)
	finishChannel=make(chan struct{},100)
	go func() {
		for item:=range statementChannel {
			fmt.Println(DeleteCheck(item.(types.DeleteStatement)))
			finishChannel<- struct{}{}
		}
	}()
	b.Run("simple delete check", func(b *testing.B) {
		for i:=0;i<b.N;i++{
			for _,statement:=range deleteStatement {
				err:=parser.Parse(strings.NewReader(statement),statementChannel)
				fmt.Println(err)
				<-finishChannel
			}
		}
	})




}
