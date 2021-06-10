# Buffer manager接口

buffer manager控制与文件交互的 block 的控制，可以看做一个 cache。

因为我们没有抽象层，所以 buffer 内部的编号不对外暴露，只有全局的 block_id 暴露



read 从文件中读取一个记录 给定文件名，block id， offset

write 写入文件一个记录 给定文件名， block id， offset

pin 锁住一个block 给定 block id

unpin 解锁 给定 block id

flush 强制写回 给定 block id

flushall 全部强制写回 无参数

```go
//所有的block_id均为全局ID，内部ID不对外开放，bool返回值表示是否操作成功。
func RecordReader(filename string, block_id int) byte[]
func RecordWriter(filename string, block_id int, record byte[]) bool
func BlockPinner(block_id int) bool
func BlockUnpinner(block_id int) bool
func BlockFlusher(block_id int) bool
func BlockFlushall() bool
```



## 之后

有需求可以实现互斥锁

