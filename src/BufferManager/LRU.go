package BufferManager

const InitSize = 1024

type LRUList struct {
	root Block
	len  int
}

func NewLRUList() *LRUList {
	l := new(LRUList)
	l.root.next = &l.root
	l.root.prev = &l.root
	l.len = 0
	return l
}

func (l *LRUList) Len() int {
	return l.len
}

func (l *LRUList) Front() *Block {
	if l.len == 0 {
		return nil
	}
	return l.root.next
}

func (l *LRUList) insert(e, at *Block) *Block {
	n := at.next
	at.next = e
	e.prev = at
	e.next = n
	n.prev = e
	l.len++
	return e
}

func (l *LRUList) remove(e *Block) *Block {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil
	e.prev = nil
	l.len--
	return e
}
func (l *LRUList) moveToBack(e *Block) {
	if l.root.prev == e {
		return
	}
	//fmt.Println(e)
	l.insert(l.remove(e), l.root.prev)
}

func (l *LRUList) appendToBack(e *Block) {
	l.insert(e, l.root.prev)
}

type LRUCache struct {
	Size     int
	root     *LRUList
	blockMap map[int]*Block
}

func NewLRUCache() *LRUCache {
	cache := new(LRUCache)
	cache.Size = InitSize
	cache.root = NewLRUList()
	cache.blockMap = make(map[int]*Block, InitSize*2)
	return cache
}

func (cache *LRUCache) PutBlock(value *Block, index int) {
	if _, ok := cache.blockMap[index]; ok {
		//fmt.Println(index)
		cache.blockMap[index] = value
		cache.root.moveToBack(value)
		return
	}
	//maybe it's wrong, I'm not sure
	if len(cache.blockMap) >= cache.Size {
		var temp = cache.root.Front()
		if temp != nil {
			for ; temp.pin; temp = temp.next {
			}
			temp.mutex.Lock()
			defer temp.mutex.Unlock()
			temp.flush()
			cache.root.remove(temp)
			oldIndex := Query2Int(nameAndPos{fileName: temp.filename, blockId: temp.blockid})
			delete(cache.blockMap, oldIndex)
		}

	}

	cache.root.appendToBack(value)

	//fmt.Println(index)
	cache.blockMap[index] = value
}

func (cache *LRUCache) GetBlock(pos int) (bool, *Block) {

	if node, ok := cache.blockMap[pos]; ok {
		cache.root.moveToBack(node)
		return true, node
	}
	return false, nil
}
