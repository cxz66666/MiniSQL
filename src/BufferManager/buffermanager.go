package BufferManager

import (
	"fmt"
	"os"
	"sync"
)

const GOFlushNum = 5  //最多多少协程同时flush
const BlockSize = 8192 // for debug
var blockBuffer *LRUCache
var connector = "*"

var posNum = 0
var fileNamePos2Int map[nameAndPos]int

var blockNumLock sync.Mutex  //没法用读写锁，必须用互斥锁
var query2IntLock sync.Mutex //struct 2 int 映射表

type nameAndPos struct {
	fileName string
	blockId  uint16
}

//InitBuffer It used at the beginging of program!!!
func InitBuffer() {
	blockBuffer = NewLRUCache()
	fileNamePos2Int = make(map[nameAndPos]int, InitSize*4)
	posNum = 0
}

//BlockRead 读byte，不检查block id和filename， 同时加互斥锁!!
func BlockRead(filename string, block_id uint16) (*Block, error) {
	t := Query2Int(nameAndPos{fileName: filename, blockId: block_id})
	ok, block := blockBuffer.GetBlock(t)
	if ok {
		block.Lock()
		return block, nil
	}
	newBlock := Block{
		blockid:  block_id,
		filename: filename,
		Data:     make([]byte, BlockSize),
	}
	err := newBlock.read()
	if err != nil {
		return nil, err
	}
	blockPtr := blockBuffer.PutBlock(&newBlock, t)
	blockPtr.Lock()
	return blockPtr, nil
}

//GetBlockNumber 返回当前总共多少个块，文件大小一定是BlockSize的倍数，文件大小 = BlockNumber * BlockSize
func GetBlockNumber(fileName string) (uint16, error) {
	blockNumLock.Lock()
	defer blockNumLock.Unlock()
	return findBlockNumber(fileName)
}

//NewBlock 返回的 block id 是指新的块在文件中的 block id
//该函数是插入用的，已经支持并发操作
func NewBlock(filename string) (uint16, error) {
	blockNumLock.Lock()
	defer blockNumLock.Unlock()
	block_id, err := findBlockNumber(filename)
	if err != nil {
		return 0, err
	}
	newBlock := Block{
		blockid:  block_id,
		filename: filename,
		Data:     make([]byte, BlockSize),
	}
	newBlock.SetDirty()
	newBlock.flush()
	t := Query2Int(nameAndPos{fileName: filename, blockId: block_id})
	blockBuffer.PutBlock(&newBlock, t)
	return block_id, nil
}

//BlockFlushAll 刷新所有块，一般不使用，拿锁， 同时协程处理
func BlockFlushAll() (bool, error) {
	blockBuffer.Lock()
	defer blockBuffer.Unlock()
	flushChannel := make(chan int)
	for i := 0; i < GOFlushNum; i++ { //开启GOFlushNum个处理协程
		go func(channel chan int) {
			for id := range channel {
				item := blockBuffer.blockMap[id]
				item.Lock()
				item.flush()
				item.reset()
				item.Unlock()
			}
		}(flushChannel)
	}
	for index, item := range blockBuffer.blockMap {
		if item.dirty {
			flushChannel <- index //传入key
		}
	}
	return true, nil
}

//BeginBlockFlush 每次结束一条指令后 channel接收指令并且开始刷新
func BeginBlockFlush(channel chan struct{}) {
	for _ = range channel {
		_, err := BlockFlushAll()
		if err != nil {
			fmt.Println(err)
		}
	}
}

//DeleteOldBlock 当删除某表时候，删除该表出现的block 首先要拿锁*
func DeleteOldBlock(fileName string) error {
	blockBuffer.Lock()
	defer blockBuffer.Unlock()
	for index, item := range blockBuffer.blockMap {
		if item.filename == fileName {
			item.Lock()
			delete(blockBuffer.blockMap, index)
			blockBuffer.root.remove(item)
			item.Unlock()
		}
	}
	return nil
}

//Query2Int 将filename和pos转为 buffer内部的int，如果不存在就创建
func Query2Int(pos nameAndPos) int {
	query2IntLock.Lock()
	defer query2IntLock.Unlock()
	if index, ok := fileNamePos2Int[pos]; ok {
		return index
	}
	posNum++
	fileNamePos2Int[pos] = posNum
	return posNum
}

func findBlockNumber(fileName string) (uint16, error) {
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		return 0, err
	}
	//fmt.Println("size is ",fileInfo.Size())
	tmp := fileInfo.Size() / BlockSize
	return uint16(tmp), nil
}
