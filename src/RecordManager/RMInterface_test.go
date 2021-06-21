package RecordManager

import (
	"fmt"

	"testing"
)

func TestCreateTable(t *testing.T) {
	err := CreateTable("student")
	fmt.Println(err)
}
