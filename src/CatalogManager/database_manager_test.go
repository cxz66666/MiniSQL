package CatalogManager

import (
	"fmt"
	"strconv"
	"testing"
)

func TestCreateDatabase(t *testing.T) {
	LoadDbMeta()
	fmt.Println(minisqlCatalog)
	CreateDatabase("123123")
	CreateDatabase("4564546")
	fmt.Println(minisqlCatalog)
}
func TestUseDatabase(t *testing.T) {
	LoadDbMeta()
	fmt.Println(minisqlCatalog)
	fmt.Println(UseDatabase("123123"))
	fmt.Println(UsingDatabase)

}

func TestDropDatabase(t *testing.T) {
	LoadDbMeta()
	fmt.Println(CreateDatabase("123123"))
	fmt.Println(CreateDatabase("12133123"))
	fmt.Println(CreateDatabase("12312123"))
	fmt.Println(CreateDatabase("122343123"))
	fmt.Println(UseDatabase("123123"))
	fmt.Println(UsingDatabase,minisqlCatalog.Databases)
	strs:=make([]DatabaseCatalog,len(minisqlCatalog.Databases))
	copy(strs,minisqlCatalog.Databases)
	for _,item:=range strs{
		fmt.Println(item)
		fmt.Println(DropDatabase(item.DatabaseId))
	}
	LoadDbMeta()
	fmt.Println(UsingDatabase,minisqlCatalog.Databases)
	FlushDbMeta()
	LoadDbMeta()
	fmt.Println(UsingDatabase,minisqlCatalog.Databases)

}
func BenchmarkCreateDatabase(b *testing.B) {
	LoadDbMeta()
	for i := 0; i < b.N; i++ {
		DropDatabase(strconv.Itoa(i))
	}
	LoadDbMeta()
	fmt.Println(UsingDatabase,minisqlCatalog.Databases)

}