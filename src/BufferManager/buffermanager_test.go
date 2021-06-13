package BufferManager

import (
	"fmt"
	"math/rand"
	"time"

	//"math/rand"
	"testing"
	//"time"
)

func TestNewBlock(t *testing.T) {
	InitBuffer()
	filename:="database1"
	for i:=1;i<=2*InitSize+10;i++ {
		fmt.Println(NewBlock(filename))
	}
}
func TestBlockRead(t *testing.T) {
	InitBuffer()
	filename:="database1"
	fmt.Println(GetBlockNumber(filename))
	r := rand.New(rand.NewSource(time.Now().Unix()))
	newdata:=[]byte("I love syf")
	for i:=1;i<2000;i++ {
		//tmp:=uint16(i%200)
		tmp:= uint16(r.Intn(2*InitSize))
		block,err:=BlockRead(filename,tmp)
		if err!=nil {
			fmt.Println(err)
			return
		}
		num:=copy(block.Data,newdata)
		fmt.Println("copy total ",num)
		block.SetDirty()
		block.FinishRead()
	}
	BlockFlushAll()
}
func BenchmarkNewBlock(b *testing.B) {
	InitBuffer()
	filename:="database1"
	for a:=0;a<b.N;a++ {
		NewBlock(filename)
	}
}

func BenchmarkBlockRead(b *testing.B) {
	InitBuffer()
	filename:="database1"
	//t:=Query2Int(nameAndPos{fileName: filename,blockId: 1})
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for i:=0;i<b.N;i++ {
		tmp:= uint16(r.Intn(8000))

		block,_:=BlockRead(filename,tmp)
		block.FinishRead()
	}
}