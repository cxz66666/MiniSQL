package BufferManager

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	//"math/rand"
	"testing"
	//"time"
)

func TestNewBlock(t *testing.T) {
	InitBuffer()
	filename:="database1"
	wg:=sync.WaitGroup{}
	wg.Add(InitSize)
	for i:=1;i<=InitSize;i++ {
		go func() {
			defer wg.Done()
			fmt.Println(NewBlock(filename))
		}()
	}
	wg.Wait()
}
func TestBlockRead(t *testing.T) {
	InitBuffer()
	//t.Parallel()

	filename:="database1"
	fmt.Println(GetBlockNumber(filename))
	r := rand.New(rand.NewSource(time.Now().Unix()))
	newdata:=[]byte("I love syf")
	//for i:=1;i<2000;i++ {
	//	//tmp:=uint16(i%200)
	//	tmp:= uint16(r.Intn(2*InitSize))
	//	block,err:=BlockRead(filename,tmp)
	//	if err!=nil {
	//		fmt.Println(err)
	//		return
	//	}
	//	num:=copy(block.Data,newdata)
	//	fmt.Println("copy total ",num)
	//	block.SetDirty()
	//	block.FinishRead()
	//}
	//BlockFlushAll()
	wg:=sync.WaitGroup{}
	wg.Add(1000)
	for i:=1;i<=1000;i++ {
		go func() {
			defer wg.Done()
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
		}()
	}
	wg.Wait()
}
func BenchmarkNewBlock(b *testing.B) {
	InitBuffer()
	filename:="database1"
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fmt.Println(NewBlock(filename))
		}
	})
}

func BenchmarkBlockRead(b *testing.B) {
	InitBuffer()
	filename:="database1"
	//t:=Query2Int(nameAndPos{fileName: filename,blockId: 1})
	r := rand.New(rand.NewSource(time.Now().Unix()))

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tmp:= uint16(r.Intn(10000))
			block,_:= BlockRead(filename,tmp)
			fmt.Println(filename,tmp)
			block.FinishRead()
		}
	})

}