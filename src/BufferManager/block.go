package BufferManager

import (
	"io"
	"os"
	"sync"
)

type Block struct {
	Filename string
	BlockId uint16
	Dirty bool
	pin bool
	Data []byte
	next *Block
	prev *Block
	mutex sync.Mutex
}

func (b *Block)reset()  {
	b.Dirty=false
	b.pin=false
	b.Data=nil
}

func (b *Block) mark(fileName string,bid uint16)  {
	b.Filename=fileName
	b.BlockId=bid
}

func (b *Block)flush() error {
	if !b.Dirty{
		return nil
	}
	file,err:=os.OpenFile(b.Filename,os.O_WRONLY,0666)
	defer file.Close()
	if err!=nil{
		return err
	}
	_,err=file.Seek(int64(b.BlockId*BlockSize),0)
	if err!=nil{
		return err
	}
	_,err=file.Write(b.Data)
	return err
}
func (b *Block)Read() error {
	if b.Dirty {
		b.Dirty=false
		return b.flush()
	}
	file,err:=os.Open(b.Filename)
	defer  file.Close()
	if err!=nil {
		return err
	}
	file.Seek(int64(b.BlockId*BlockSize),0)

	b.Data=make([]byte, BlockSize)
	_, err = io.ReadFull(file, b.Data)
	return err
}
func (b *Block)SetDirty() {
	b.Dirty=true
}

func (b *Block)PinBlock()  {
	b.pin=true
}

func (b *Block)UnPinBlock()  {
	b.pin=false
}