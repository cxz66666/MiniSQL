package CatalogManager

import (
	"fmt"
	"minisql/src/Interpreter/types"
	"testing"
)

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
				1,
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
func TestCreateTable(t *testing.T) {
	LoadDbMeta()
	fmt.Println(CreateDatabase("123123"))
	fmt.Println(CreateDatabase("4564546"))
	fmt.Println(UseDatabase("4564546"))
	for _,item:=range createtable {
		fmt.Println(CreateTable(item))
	}
	for k,v:=range TableCatalogMap {
		fmt.Println(k,*v)
	}
	fmt.Println(UseDatabase("123123"))
	fmt.Println(UseDatabase("4564546"))

	for k,v:=range TableCatalogMap {
		fmt.Println(k,*v)
	}
}
func BenchmarkCreateTable(b *testing.B) {
	LoadDbMeta()
	for i:=0;i<b.N;i++ {
		for _,item:=range createtable {
			err:= CreateTableStatement2TableCatalog(&item)
			println(err)
		}
	}
}