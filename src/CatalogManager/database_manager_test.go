package CatalogManager

import (
	"fmt"
	"testing"
)

func TestCreateDatabase(t *testing.T) {
	CreateDatabase("123123")
	CreateDatabase("4564546")

}
func TestUseDatabase(t *testing.T) {
	LoadDbMeta()
	fmt.Println(minisqlCatalog)
	fmt.Println(UseDatabase("123123"))
	fmt.Println(UsingDatabase)
}