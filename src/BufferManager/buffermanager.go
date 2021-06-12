package BufferManager

import (
	"errors"
	"os"
)

const BlockSize = 4096
var blockBuffer  *LRUCache
var connector="*/"


//It used at the beginging of program!!!
func InitBuffer()  {
	blockBuffer=NewLRUCache()
}
//读byte，不检查block id和filename， 同时加互斥锁
func BlockRead(filename string, block_id uint16) ([]byte, error) {
	ok,block:=blockBuffer.GetBlock(connectNameId(filename,block_id))
	if ok {
		block.mutex.Lock()
		return block.Data,nil
	}
	newBlock:=Block{
		BlockId: block_id,
		Filename: filename,
	}
	err:=newBlock.Read()
	if err!=nil{
		return nil,err
	}
	blockBuffer.PutBlock(&newBlock)
	newBlock.mutex.Lock()
	return newBlock.Data,nil
}
//释放互斥锁
func BlockFinishRead(filename string, block_id uint16) error {
	ok,block:=blockBuffer.GetBlock(connectNameId(filename,block_id))
	if !ok {
		return errors.New("can't find the block_id "+string(block_id))
	}
	block.mutex.Unlock()
	return nil
}
//返回总共多少个块，一定是4KB的倍数
func GetBlockNumber(fileName string) (uint16,error)  {
	fileInfo, err:= os.Stat(fileName)
	if err!=nil	{
		return 0,err
	}
	return uint16(fileInfo.Size()/BlockSize),nil
}
// 返回的 block id 是指新的块在文件中的 block id
func NewBlock(filename string) (uint16, error) {
	block_id,err:=GetBlockNumber(filename)
	if err!=nil	{
		return 0,err
	}
	newBlock:=Block{
		BlockId: block_id,
		Filename: filename,
		Data: make([]byte,BlockSize),
	}
	newBlock.flush()
	blockBuffer.PutBlock(&newBlock)
	return block_id,nil
}


func BlockFlushAll() (bool, error) {

}

func SetDirty(fileName string,blockId uint16)  {

}



func connectNameId(fileName string,blockId uint16)string{
	return fileName+string([]byte {byte(blockId >> 8), byte(blockId & 0xFF)})
}
