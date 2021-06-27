package BufferManager

import (
	"sync"
)

const InitSize = 2048 //块多少
const DeleteSize = 512 //满了后就删除多少个
type LRUList struct {
	root Block  // dummy header
	len  int
}

func NewLRUList() *LRUList {
	l := new(LRUList)
	l.root.next = &l.root  //a loop
	l.root.prev = &l.root // a loop
	l.len = 0
	return l
}

func (l *LRUList) Len() int {
	return l.len
}
//Front 返回链表的头
func (l *LRUList) Front() *Block {
	if l.len == 0 {
		return nil
	}
	return l.root.next
}
//insert 插入某block在at之后
func (l *LRUList) insert(e, at *Block) *Block {
	n := at.next
	at.next = e
	e.prev = at
	e.next = n
	n.prev = e
	l.len++
	return e
}
//remove 删除某节点
func (l *LRUList) remove(e *Block) *Block {
	e.prev.next = e.next
	e.next.prev = e.prev
	//e.next = nil
	//e.prev = nil
	l.len--
	return e
}
//moveToBack 访问过后放到链表尾部
func (l *LRUList) moveToBack(e *Block) {
	if l.root.prev == e {
		return
	}
	//fmt.Println(e)
	l.insert(l.remove(e), l.root.prev)
}
//新建的block
func (l *LRUList) appendToBack(e *Block) {
	l.insert(e, l.root.prev)
}
//LRUCache is the cache struct
type LRUCache struct {
	Size     int
	root     *LRUList
	 sync.Mutex
	blockMap map[int]*Block
}

func NewLRUCache() *LRUCache {
	cache := new(LRUCache)
	cache.Size = InitSize
	cache.root = NewLRUList()
	cache.blockMap = make(map[int]*Block, InitSize*2)
	return cache
}
//PutBlock 将block插入buffer中，并返回block的指针
func (cache *LRUCache) PutBlock(value *Block, index int) *Block {
	cache.Lock() //写锁
	defer cache.Unlock()
	if item, ok := cache.blockMap[index]; ok {
		//fmt.Println(index)
		cache.root.moveToBack(item)
		return item
	}
	//maybe it's wrong, I'm not sure
	if len(cache.blockMap) >= cache.Size {
		var temp = cache.root.Front()

		for deleteNum:=0;deleteNum<DeleteSize;deleteNum++ {
			if temp.pin{
				temp=temp.next
			} else {
				temp.Lock()
				temp.flush()
				cache.root.remove(temp)
				oldIndex := Query2Int(nameAndPos{fileName: temp.filename, blockId: temp.blockid})
				delete(cache.blockMap, oldIndex)
				temp.Unlock()
				temp=temp.next
			}
		}
	}
	cache.root.appendToBack(value)

	//fmt.Println(index)
	cache.blockMap[index] = value
	return  value
}
//GetBlock 获取buffer中的缓存，如果没找到就返回false
func (cache *LRUCache) GetBlock(pos int) (bool, *Block) {
	cache.Lock() //读锁
	defer cache.Unlock()
	if node, ok := cache.blockMap[pos]; ok {
		cache.root.moveToBack(node)
		return true, node
	}
	return false, nil
}
