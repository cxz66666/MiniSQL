package CatalogManager

import (
	"errors"
	"github.com/tinylib/msgp/msgp"
	"minisql/src/Utils"
	"os"
)
var(
	minisqlCatalog  MiniSqlCatalog
)
const MiniSqlCatalogPos = FolderPosition+MiniSqlCatalogName
//LoadDbMeta is used for init the database catalog
//And if file is not exits, it will create it and return nil
func LoadDbMeta() error {

	if !Utils.Exists(FolderPosition) {
		err:=Utils.CreateDir(FolderPosition);
		if err!=nil {
			return errors.New("无法创建根文件夹")
		}
		f,err:= Utils.CreateFile(MiniSqlCatalogPos)
		defer f.Close()
		if err!=nil {
			return errors.New("无法创建索引文件")
		}
		newCatalog:=MiniSqlCatalog{}
		wt:=msgp.NewWriter(f)
		err=newCatalog.EncodeMsg(wt)
		return err
	}

	f,err:=os.OpenFile(MiniSqlCatalogPos,os.O_RDWR,0666)
	defer f.Close()
	if err!=nil{
		return errors.New("根索引文件打开失败")
	}
	rd:=msgp.NewReader(f)
	err=minisqlCatalog.DecodeMsg(rd)
	if err!=nil {
		return errors.New("根索引读取失败，请尝试重启系统")
	}
	return nil
}

func FlushDbMeta() error {
	f,err:=os.OpenFile(MiniSqlCatalogPos,os.O_RDWR,0666)
	defer f.Close()
	if err!=nil{
		return errors.New("文件打开失败")
	}
	wt:=msgp.NewWriter(f)
	err=minisqlCatalog.EncodeMsg(wt)
	if err!=nil {
		return errors.New("根索引写入失败")
	}
	return wt.Flush()
}
