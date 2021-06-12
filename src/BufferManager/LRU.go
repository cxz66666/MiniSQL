package BufferManager
const InitSize=256

type LRUList struct {
	root Block
	len int
}

func NewLRUList()*LRUList {
	l := new(LRUList)
	l.root.next = &l.root
	l.root.prev = &l.root
	l.len = 0
	return l
}

func (l *LRUList) Len() int {
	return l.len
}

func (l *LRUList)Front()*Block  {
	if l.len==0{
		return nil
	}
	return l.root.next
}

func (l *LRUList)insert(e,at *Block)*Block  {
	n := at.next
	at.next = e
	e.prev = at
	e.next = n
	n.prev = e
	l.len++
	return e
}

func (l *LRUList)remove(e *Block)*Block  {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil
	e.prev = nil
	l.len--
	return e
}
func (l *LRUList)moveToBack(e *Block)  {
	if l.root.prev == e {
		return
	}
	l.insert(l.remove(e), l.root.prev)
}

func (l *LRUList)appendToBack(e *Block)  {
	l.insert(e, l.root.prev)
}


type LRUCache struct {
	Size int
	root *LRUList
	blockMap map[string]*Block
}

func NewLRUCache()*LRUCache  {
	cache:=new(LRUCache)
	cache.Size=InitSize
	cache.root=NewLRUList()
	cache.blockMap=make(map[string]*Block,InitSize)
	return cache
}

func (cache *LRUCache) PutBlock(value *Block) {
	var nameAndId=connectNameId(value.Filename,value.BlockId)
	if 	_,ok:=cache.blockMap[nameAndId];ok {
		cache.blockMap[nameAndId]=value
		cache.root.moveToBack(value)
		return
	}
	//maybe it's wrong, I'm not sure
	if cache.Size>=len(cache.blockMap) {
		var temp *Block
		for temp=cache.root.Front();temp.Pin;temp=temp.next{

		}
		temp.mutex.Lock()
		defer temp.mutex.Unlock()
		temp.flush()
		cache.root.remove(temp)
		delete(cache.blockMap,nameAndId)
	}

	cache.root.appendToBack(value)
	cache.blockMap[nameAndId]=value
}

func (cache *LRUCache)GetBlock(nameAndId string) (bool,*Block)  {
	if node,ok:=cache.blockMap[nameAndId];ok {
		return true,node
	}
	return false,nil
}