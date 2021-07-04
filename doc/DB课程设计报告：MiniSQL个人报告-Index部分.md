# DB课程设计报告：MiniSQL个人报告

### 一、 实验目的   

设计并实现一个精简型单用户 SQL 引擎(DBMS)MiniSQL，允许用户通过字符界面输入 SQL 语句实现表的建立/删除;索引 的建立/删除以及表记录的插入/删除/查找。通过对 MiniSQL 的设计与实现，提高学生的系统编程能力，加深对数据库系统原理的理解。

我在本次实验中主要负责的内容是IndexManager，以及bonus（gui方面）的制作。

### 二、 系统需求

1. 基本需求：表的创建、删除，记录的创建、删除，索引的创建删除，基本的错误显示，重点部分是记录的查询（要求支持等值查询、区间查询、以及使用and链接的多个条件的查询）
2. 数据类型：要求至少支持int、float、char
3. 高标准要求（bonus）：中文支持、GUI、更加丰富的错误显示、更多的语句等。

在本次，我们完全覆盖了基本需求和几乎全部的Bonus。

### 三、 实验环境

**1. 操作系统**

​# MiniSQL 个人报告

组别：第七组

个人分工： Index Manager

由于大程本身时间花费较多，而考试紧迫，报告时间仓促，我就尽量从简了。

## Index Manager 逻辑层面的技术细节

我们的教材上其实给出了 B+ 树操作的写法，但我在实现的时候还是参考了算法导论上的方法。其主要区别是，数据库教材上 B+ 树的操作是自顶向下、再自底向上的两遍操作，而我实现的 B+ 树只需要自顶向下的一遍操作。这两种方法的优劣我会在后面进行比较说明

为了实现自顶向下一遍操作，我必须保证当在插入或者删除到前节点的时候，绝对不会产生节点的上溢或者下溢。这就需要在这个节点的上一层节点就进行检查，保证在当前节点不会出现问题。而根节点没有上一层节点，所以根节点的情况需要特别判断，综合起来，我的伪代码如下：

Insert:

```pseudocode
function insert(key, value)
	if root is full
		split_root()
	cur_root := root
	while (cur_root is not leaf)
		find the first key[i] > key
		if (cur_root.child[i] is full)
			split(cur_root, i)
		cur_root = cur_root.child[i]
	find the first key[i] > key
	insert (key, value) into ith position
```

Delete: 由于我们只在 unique 上建索引，所以只需要提供 key 就好了。这里记 danger 表示 某个节点拥有的 key 值个数等于 $\lceil Order/2\rceil$ 

```pseudocode
function delete(key)
	if root is danger
		delete_root()
	cur_root := root
	while (cur_root is not leaf)
		find the first key[i] > key
		cur_root = cur_root.child[i]
		if (cur_root.child[i] is danger)
			handle(cur_root, i)
	find the first key[i] = key
	delete (key[i], value[i]) from cur_node
```

如果节点满了，我会进行分裂操作，分裂操作的核心操作在于：对于叶子节点，我们需要把分裂之后的第二个节点的第一个元素添加到父节点上作为 pivot；对于非叶子节点，我们需要把中间的元素提上去作为父节点中的 pivot

```pseudocode
function split(cur_root, k)
	if cur_root.child[k] is leaf
		create a new node new_node
		copy the latter half of cur_root.child[k] into new_node
		insert (new_node.key[0], pointer to new_node) into cur_root
	else
		create a new node new_node
        copy the latter half of cur_root.child[k] into new_node
        insert (cur_node.child[i].key[median], pointer to new_node) into the middle of cur_root
```

根节点的分裂稍有不同，我采取的做法是直接新建一个node，让它指向原来的 root，这样后面的操作就可以正常执行了

```pseudocode
function split_root()
	create a new node new_node
	new_node.P[0] = root
```

这样，在后面的循环中我们会把根节点作为这个新建的节点的唯一一个子节点进行分裂，解决了根节点没有父节点就无法分裂的问题。

合并和分裂的操作类似，但区别在于合并未必每次都会进行，因为有可能左右两个节点与它的节点大小之和大于一个节点能够承受的，所以在这个时候我们首先会进行调整

```pseudocode
function handle(cur_root, k)
	if cur_root.child[k + 1].size() + cur_root.child[k].size() < Order
		copy cur_root.child[k + 1] into the tail of cur_root.child[k]
		delete cur_root.child[k]
	else 
		move the cur_root.key[k] into cur_root.child[k]
		move cur_root.child[k + 1].key[0] into cur_root.child[k].key[last]
```

这里我做了一些简化，实际上还要考虑$k = size$ 的情况，此时需要和前面的节点做合并或者调整

Search：

针对一共五种不同类型的查找，我的处理方法分别如下：

1. 小于、小于等于：直接找到第一个节点，利用叶子节点之间相互连接的特性，顺序查找，直到第一个不满足条件的被遇到
2. 等于：顺着 B+ 树找到第一个大于等于次元素的元素，如果是大于，返回空
3. 大于、大于等于：顺着B+树找到第一个大于等于此元素的元素，如果是大于，继续找到第一个大于此元素的元素

由于 Search 相对简单，就不提供伪代码了。

相比于书上提供的那种自顶向下再自底向上的处理方法，我的写法只需要一边扫描，单次操作的常数更小，但是考虑到我的写法会分裂或者合并不必要分裂和合并的节点，所以平均下来时间复杂度可能差不多。此外就是我的写法可能会产生更多半满的节点（因为分裂比书上的写法更频繁），相比书上的写法，空间消耗可能更大。

## Index 物理层面的技术细节

由于上面涉及到的都是在内存中的操作，而实际上我们的 index 是要持久化、写入存储的，因此这里面涉及到了和 Buffer 的交互，我采用一个 block 记录一个节点的方法，内部的信息如下：

叶子节点：

| block | 信息             |
| ----- | ---------------- |
| [0]   | IsLeaf(uint8)    |
| [1:2] | Size(uint16)     |
| [3:4] | NextNode(uint16) |
| [5:8] | Pointer[0]       |
| [9:?] | Key[0]           |
| ...   | ...              |

非叶子节点：

| block | 信息          |
| ----- | ------------- |
| [0]   | IsLeaf(uint8) |
| [1:2] | Size(uint16)  |
| [3:4] | Pointer[0]    |
| [5:?] | Key[0]        |
| ....  |               |

其中，叶子节点的 pointer 需要记录 block + offset， 而非叶子节点的 pointer 只需要记录 block，因为一个 block 就是一个节点。

利用这样的组织，我实现了getXXX系列的函数和 setXXX 系列的函数，分别用来获取和设置各个字段。

