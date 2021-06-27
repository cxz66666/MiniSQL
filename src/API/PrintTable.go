package API

import (
	"github.com/jedib0t/go-pretty/v6/table"
	"minisql/src/Interpreter/value"
	"os"
	"strconv"
)
//PrintTable 接收表名，列名，各个记录，打印table
func PrintTable(tableName string,columnName value.Row,records []value.Row) error  {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	totalHeader:=make([]interface{},0,len(columnName.Values)+1)
	totalHeader=append(totalHeader,tableName)
	for _,item:=range columnName.Values {
		totalHeader=append(totalHeader,item.String())
	}
	t.SetStyle(table.StyleColoredBright)
	t.AppendHeader(totalHeader)
	columnNum:=len(columnName.Values)

	Rows:=make([]table.Row,0,len(records)+1)

	for i,item:=range records{
		newRow:=make([]interface{},0,columnNum+1)
		newRow=append(newRow,strconv.Itoa(i))
		for _,col:=range item.Values {
			newRow=append(newRow,col.String())
		}
		Rows=append(Rows, newRow)
	}
	t.AppendRows(Rows)
	t.AppendFooter(table.Row{"Total", len(records)})
	t.Render()
	return nil
}