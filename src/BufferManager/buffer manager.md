# Buffer manager接口

buffer manager控制与文件交互的 block 的控制，可以看做一个 cache。

buffer内部编号为int类型，外界输入的filename和blockId拼接会唯一对应一个int索引，该索引并不是buffer内部的编号，因为buffer内部为链表+map，并非数组，所以该索引只是起到代替string的作用，加速查找



#### Block类

- FileName、block_id初始时会写入
- dirty代表该块是否被写过，如果写过请使用SetDirty方法
- pin代表该块是否被pin住，如果要pin某个块，请使用PinBlock方法，同样使用UnPinBlock方法解锁
- Data为对外暴露的数据，为大小4KB的切片，可以直接以你喜欢的方式修改，但是修改时请使用SetDirty置为脏
- mutex为互斥锁，当使用getBlock拿到该block的指针时，bm会自动给block上锁，因此使用完一个block后**务必**使用FinishRead释放锁，不然就会产生死锁再也无法拿到



暴露方法：

- SetDirty
- PinBlock
- UnPinBlock
- ==FinishRead==

内部方法（无需外部调用）：

- read 读取filename和blockid指定的一段内容，如果该block为dirty，则不会read，而是会flush
- flush 如果不为dirty，直接返回，否则写入data
- mark 置filename和bid位
- reset 重置filename和bid



#### BM对外暴露函数

以下内容全部不检查错误，如果按规则来基本不会产生错误

```
//读取指定filename和bid，同时加互斥锁 失败返回err
func BlockRead(filename string, block_id uint16) (*Block, error)
```



```
//获取当前文件有多少块，拿到后0——t-1为可用区间
func GetBlockNumber(fileName string) (uint16,error)  
```



```
//新加一块，同时加入缓存，返回blockid
func NewBlock(filename string) (uint16, error) 
```



```
//强制刷新所有块，但是不会清出缓存
func BlockFlushAll() (bool, error) 
```





## 之后

有需求可以实现读写锁

