package BufferManager

import (
	"io"
	"os"
	"sync"
)

// Block 为缓冲区的块，只对外保留Data切片
type Block struct {
	filename string
	blockid  uint16
	dirty    bool
	pin      bool
	Data     []byte
	next     *Block  //后继
	prev     *Block  //前驱
	    sync.Mutex
}

//脏了
func (b *Block)SetDirty() {

	b.dirty =true
}
//pin住留在缓冲区内
func (b *Block)PinBlock()  {
	b.pin=true
}
//解pin
func (b *Block)UnPinBlock()  {
	b.pin=false
}
//释放读锁，读完一块必须干此时，不然锁就无法释放
func (b *Block)FinishRead()  {
	b.Unlock()
	return
}

func (b *Block)reset()  {
	b.dirty =false
	b.pin=false
}

func (b *Block) mark(fileName string,bid uint16)  {
	b.filename =fileName
	b.blockid =bid
}

func (b *Block)flush() error {
	if !b.dirty {
		return nil
	}
	file,err:=os.OpenFile(b.filename,os.O_WRONLY,0666)
	defer file.Close()
	if err!=nil{
		return err
	}
	bid64:=int64(b.blockid)
	_,err=file.Seek(bid64*BlockSize,0)
	if err!=nil{
		return err
	}
	_,err=file.Write(b.Data)
	b.dirty =false

	return err
}
func (b *Block)read() error {
	if b.dirty {
		return b.flush()
	}
	file,err:=os.Open(b.filename)
	defer  file.Close()
	if err!=nil {
		return err
	}
	bid64:=int64(b.blockid)
	file.Seek(bid64*BlockSize,0)

	_, err = io.ReadFull(file, b.Data)
	return err
}

