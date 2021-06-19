package CatalogManager

import (
	"fmt"
	"minisql/src/Interpreter/parser"
	"minisql/src/Interpreter/types"
	"strconv"
	"strings"
	"testing"
)

var droptable=[]types.DropTableStatement{
	{
		"1",
	},
	{
		"2",
	},
}
var createtable=[]types.CreateTableStatement{
	{
		"cxz666",
		map[string]types.Column{
			"first":{
				"first",
				types.ColumnType{
					types.Int64,
					8,
					false,
				},
				false,
				false,
				2,
			},
			"second":{
				"second",
				types.ColumnType{
					types.Float64,
					8,
					false,
				},
				false,
				false,
				0,
			},
			"third":{
				"third",
				types.ColumnType{
					types.Bytes,
					30,
					false,
				},
				false,
				false,
				3,
			},
			"fourth":{
				"fourth",
				types.ColumnType{
					types.Int64,
					8,
					false,
				},
				true,
				true,
				1,
			},
			"fifth":{
				"fifth",
				types.ColumnType{
					types.Bytes,
					40,
					false,
				},
				true,
				true,
				4,
			},
		},
		[]types.Key{
			{
				"second",
				types.Asc,
			},

		},
		types.Cluster{},
	},
}

var create_table_test_string=[]string{
	"create table cxz ( " +
		"column1 int not null,\n" +
		"column2 char(30),\n" +
		"column3 float unique,\n" +
		"column4 bytes(40),\n" +
		"column5 char(50) unique not null," +
		"primary key (column1)" +
		");",
	"create table syf (" +
		"column1 bytes(40),\n" +
		"column2 bytes(40),\n" +
		"column3 bytes(40),\n" +
		"column4 bytes(40),\n" +
		"primary key (column1)" +
		");",
}
var drop_table_test_string=[]string{
	"drop table cxz;",
	"drop table notsyf;",
	"drop table syf;",
	"drop table cxz66666;",
}
func TestCreateTable(t *testing.T) {
	LoadDbMeta()
	fmt.Println(CreateDatabase("123123"))
	fmt.Println(CreateDatabase("4564546"))
	fmt.Println(UseDatabase("4564546"))

	for _,item:=range createtable {
		fmt.Println(CreateTableCheck(item))
	}
	for k,v:=range TableName2CatalogMap {
		fmt.Println(k,*v)
	}
	fmt.Println(UseDatabase("123123"))
	for k,v:=range TableName2CatalogMap {
		fmt.Println(k,*v)
	}
	statementChannel=make(chan types.DStatements,100)
	finishChannel=make(chan struct{},100)
	go func() {
		for item:=range statementChannel {
			fmt.Println(CreateTableCheck(item.(types.CreateTableStatement)))
			finishChannel<- struct{}{}
		}
	}()
	for _,item:=range create_table_test_string {
		err:=parser.Parse(strings.NewReader(item),statementChannel)
		fmt.Println(err)
		<-finishChannel
	}
	for k,v:=range TableName2CatalogMap {
		fmt.Println(k,*v)
	}

}
func TestDropTable(t *testing.T) {
	LoadDbMeta()
	fmt.Println(UseDatabase("4564546"))
	for k,_:=range TableName2CatalogMap{
		fmt.Println(DropTable(types.DropTableStatement{TableName: k}))
	}
	fmt.Println(UseDatabase("123123"))
	statementChannel=make(chan types.DStatements,100)
	finishChannel=make(chan struct{},100)
	go func() {
		for item:=range statementChannel {
			fmt.Println(DropTableCheck(item.(types.DropTableStatement)))
			finishChannel<- struct{}{}
		}
	}()
	for _,item:=range drop_table_test_string{
		err:=parser.Parse(strings.NewReader(item),statementChannel)
		fmt.Println(err)
		<-finishChannel
	}
}
func BenchmarkDropTable(b *testing.B) {
	LoadDbMeta()
	fmt.Println(UseDatabase("4564546"))
	for i:=0;i<b.N;i++ {
		DropTable(types.DropTableStatement{TableName: strconv.Itoa(i)})
	}
}
func BenchmarkCreateTable(b *testing.B) {
	LoadDbMeta()
	fmt.Println(UseDatabase("4564546"))
	for i:=0;i<b.N;i++ {
		new_table:=createtable[0]
		new_table.TableName=strconv.Itoa(i)
		CreateTableCheck(new_table)
	}
	fmt.Println(TableName2CatalogMap)
}