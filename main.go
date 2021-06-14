package main

import (
	"bufio"
	"fmt"
	"github.com/peterh/liner"
	"minisql/src/API"
	"minisql/src/BufferManager"
	"minisql/src/CatalogManager"
	"minisql/src/Interpreter/parser"
	"minisql/src/Interpreter/types"
	"os"
	"path/filepath"
	"strings"
	"time"
)


const historyCommmandFile="~/.minisql_history"
const firstPrompt="minisql>"
const secondPrompt="      ->"

func InitDB() error {
	err:= CatalogManager.LoadDbMeta()
	if err!=nil {
		return err
	}
	BufferManager.InitBuffer()
	return nil
}
func expandPath(path string) (string,error)  {
	if strings.HasPrefix(path, "~/") {
		parts := strings.SplitN(path, "/", 2)
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		return filepath.Join(home, parts[1]), nil
	}
	return path, nil
}
func loadHistoryCommand() (*os.File,error) {
	var file *os.File
	path,err:=expandPath(historyCommmandFile)
	if err!=nil{
		return nil,err
	}
	_,err=os.Stat(path)
	if os.IsNotExist(err) {
		file,err=os.Create(path)
		if err!=nil {
			return nil,err
		}
	} else {
		file,err=os.OpenFile(path,os.O_RDWR,0666)
		if err !=nil {
			return nil,err
		}
	}
	return file,err

}
func runShell(r chan<- error)  {
	ll:=liner.NewLiner()
	defer ll.Close()
	ll.SetCtrlCAborts(true)
	file,err:= loadHistoryCommand()
	if err !=nil {
		panic(err)
	}
	defer func() {
		_,err:=ll.WriteHistory(file)
		if err !=nil{
			panic(err)
		}
		_=file.Close()
	}()
	s:= bufio.NewScanner(file)
	for s.Scan() {
		//fmt.Println(s.Text())
		ll.AppendHistory(s.Text())
	}
	InitDB()

	StatementChannel:=make(chan types.DStatements,100)
	FinishChannel:=make(chan struct{},100)
	go API.HandleOneParse(StatementChannel,FinishChannel)  //begin the runtime for exec
	var beginSQLParse=false
	var sqlText=make([]byte,0,100)
	for { //each sql
LOOP:
		beginSQLParse=false
		sqlText=sqlText[:0]
		var input string
		var err error
		for {  //each line
			if beginSQLParse{
				input, err = ll.Prompt(secondPrompt)
			} else {
				input, err = ll.Prompt(firstPrompt)
			}
			if err !=nil{
				if  err ==liner.ErrPromptAborted {
					goto  LOOP
				}
			}
			trimInput:=strings.TrimSpace(input) //get the input without front and backend space
			if len(trimInput)!=0 {
				ll.AppendHistory(input)
				if !beginSQLParse&&(trimInput=="quit"||strings.HasPrefix(trimInput,"quit;")) {
					close(StatementChannel)
					for _=range FinishChannel {

					}
					r<-err
					return
				}
				sqlText=append(sqlText,append([]byte{' '},[]byte(trimInput)[0:]...)...)
				if !beginSQLParse {
					beginSQLParse=true
				}
				if strings.Contains(trimInput,";") {
					break
				}
			}
		}
		beginTime:=time.Now()
		err=parser.Parse(strings.NewReader(string(sqlText)),StatementChannel)
		//fmt.Println(string(sqlText))
		if err != nil {
			fmt.Println(err)
			continue
		}
		<-FinishChannel
		durationTime:=time.Since(beginTime)
		fmt.Println("Finish operation at: ",durationTime)
	}
}
func main() {
	errChan:=make(chan error)
	go runShell(errChan)
	err:=<-errChan
	fmt.Println("bye")
	fmt.Println(err)
}