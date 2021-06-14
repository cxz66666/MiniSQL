package BufferManager

import (
	"os"
)

const BlockSize = 64 // for debug
var blockBuffer *LRUCache
var connector = "*"

var posNum = 0
var fileNamePos2Int map[nameAndPos]int

type nameAndPos struct {
	fileName string
	blockId  uint16
}

//It used at the beginging of program!!!
func InitBuffer() {
	blockBuffer = NewLRUCache()
	fileNamePos2Int = make(map[nameAndPos]int, InitSize*4)
	posNum = 0
}

//读byte，不检查block id和filename， 同时加互斥锁
func BlockRead(filename string, block_id uint16) (*Block, error) {
	t := Query2Int(nameAndPos{fileName: filename, blockId: block_id})
	ok, block := blockBuffer.GetBlock(t)
	if ok {
		block.mutex.Lock()
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
	blockBuffer.PutBlock(&newBlock, t)
	newBlock.mutex.Lock()
	return &newBlock, nil
}

//返回当前总共多少个块，一定是4KB的倍数
func GetBlockNumber(fileName string) (uint16, error) {
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		return 0, err
	}
	//fmt.Println("size is ",fileInfo.Size())
	tmp := fileInfo.Size() / BlockSize
	return uint16(tmp), nil
}

// 返回的 block id 是指新的块在文件中的 block id
func NewBlock(filename string) (uint16, error) {
	block_id, err := GetBlockNumber(filename)
	if err != nil {
		return 0, err
	}
	newBlock := Block{
		blockid:  block_id,
		filename: filename,
		Data:     make([]byte, BlockSize),
		dirty:    true,
	}
	newBlock.flush()
	t := Query2Int(nameAndPos{fileName: filename, blockId: block_id})
	blockBuffer.PutBlock(&newBlock, t)
	return block_id, nil
}

func BlockFlushAll() (bool, error) {
	for _, item := range blockBuffer.blockMap {
		if item.dirty {
			err := item.flush()
			if err != nil {
			}
			return false, err
		}
		item.reset()
	}
	return true, nil
}

func Query2Int(pos nameAndPos) int {
	if index, ok := fileNamePos2Int[pos]; ok {
		return index
	}
	posNum++
	fileNamePos2Int[pos] = posNum
	return posNum
}
