package IndexManager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"minisql/src/BufferManager"
	"minisql/src/Interpreter/value"
)

func (node bpNode) print(value_type value.ValueType) {
	fmt.Println("Is leaf: " + fmt.Sprint(node.isLeaf()))
	fmt.Println("Size: " + fmt.Sprint(node.getSize()))
	n := node.getSize()
	if node.isLeaf() == 0 {
		for i := uint16(0); i < n; i++ {
			fmt.Println(" " + fmt.Sprint(node.getPointer(i)) + " Key: " + fmt.Sprint(node.getKey(i, value_type)))
		}
		fmt.Println(" " + fmt.Sprint(node.getPointer(n)))
	} else {
		for i := uint16(0); i < n; i++ {
			fmt.Println("Key: " + fmt.Sprint(node.getKey(i, value_type)) + ", Pointer: " + fmt.Sprint(node.getFilePointer(i)))
		}
		fmt.Println("Next: ", node.getNext())
	}
	fmt.Println()
}

func (node bpNode) nodeInit() {
	node.setSize(0)
	node.setLeaf(1)
	node.setNext(0)
}

func getBpNode(filename string, block_id uint16, key_length uint16) (node bpNode, block *BufferManager.Block) {
	//fmt.Println("bpnode read",block_id)
	block, _ = BufferManager.BlockRead(filename, block_id)
	node = bpNode{
		key_length: key_length,
		data:       block.Data,
	}
	return
}

// Get the order of this B+ tree
// Order is supposed to be the maximum *odd* number that
// the block is capable of storing that many keys
func getOrder(key_length uint16) uint16 {
	var order uint16 = (BufferManager.BlockSize-5)/(key_length+4) - 1
	if (order & 1) == 0 {
		order--
	}
	return order
}

// Is this node a leaf?
func (node bpNode) isLeaf() uint8 {
	return uint8(node.data[0])
}

// Set the leaf property of this node
func (node bpNode) setLeaf(leaf uint8) {
	node.data[0] = byte(leaf)
}

// Get the size of the node
func (node bpNode) getSize() uint16 {
	var size uint16
	buf := bytes.NewBuffer(node.data[1:3])
	binary.Read(buf, binary.LittleEndian, &size)
	return size
}

// Set the size of the node
func (node bpNode) setSize(size uint16) {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, size)
	copy(node.data[1:3], buf.Bytes())
}

func (node bpNode) isFull(M uint16) bool {
	if node.isLeaf() == 1 {
		return node.getSize() == M+1
	} else {
		return node.getSize() == M
	}
}

func (node bpNode) isDanger(M uint16) bool {
	if node.isLeaf() == 1 {
		return node.getSize() == (M+1)/2
	} else {
		return node.getSize() == (M-1)/2
	}
}

// Get the start of the P[k]
func (node bpNode) getPointerPosition(k uint16) uint16 {
	var offset uint16
	if node.isLeaf() == 1 {
		subnode_length := node.key_length + 4
		offset = 5 + k*subnode_length
	} else {
		subnode_length := node.key_length + 2
		offset = 3 + k*subnode_length
	}
	return offset
}

// Get the start of the Key[k]
func (node bpNode) getKeyPosition(k uint16) uint16 {
	var offset uint16
	if node.isLeaf() == 1 {
		subnode_length := node.key_length + 4
		offset = 9 + k*subnode_length
	} else {
		subnode_length := node.key_length + 2
		offset = 5 + k*subnode_length
	}
	return offset
}

// Get P[k] (for internal node)
func (node bpNode) getPointer(k uint16) uint16 {
	var block_id uint16
	from := node.getPointerPosition(k)
	to := from + 2
	buf := bytes.NewBuffer(node.data[from:to])
	binary.Read(buf, binary.LittleEndian, &block_id)
	return block_id
}

// Set P[k]
func (node bpNode) setPointer(k uint16, block_id uint16) {
	from := node.getPointerPosition(k)
	to := from + 2
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, block_id)
	copy(node.data[from:to], buf.Bytes())
}

// Get P[k] (for leaf node)
func (node bpNode) getFilePointer(k uint16) Position {
	var block_id, offset uint16
	from := node.getPointerPosition(k)
	to := from + 2
	buf := bytes.NewBuffer(node.data[from:to])
	binary.Read(buf, binary.LittleEndian, &block_id)

	// Get Offset
	from += 2
	to += 2
	buf = bytes.NewBuffer(node.data[from:to])
	binary.Read(buf, binary.LittleEndian, &offset)
	return Position{
		Block:  block_id,
		Offset: offset,
	}
}

// Set P[k] (for leaf node)
func (node bpNode) setFilePointer(k uint16, pos Position) {
	from := node.getPointerPosition(k)
	to := from + 2
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, pos.Block)
	copy(node.data[from:to], buf.Bytes())

	// Get Offset
	from += 2
	to += 2
	buf = bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, pos.Offset)
	copy(node.data[from:to], buf.Bytes())

}

// Get next leaf
func (node bpNode) getNext() uint16 {
	var block_id uint16
	buf := bytes.NewBuffer(node.data[3:5])
	binary.Read(buf, binary.LittleEndian, &block_id)
	return block_id
}

// Set Next
func (node bpNode) setNext(block_id uint16) {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, block_id)
	copy(node.data[3:5], buf.Bytes())
}

// Get Key[k]
func (node bpNode) getKey(k uint16, value_type value.ValueType) value.Value {
	from := node.getKeyPosition(k)
	to := from + node.key_length
	val, err := value.Byte2Value(node.data[from:to], value_type, int(node.key_length))
	if err != nil {
		panic(err)
	} else {
		return val
	}
}

// Set Key[k]
func (node bpNode) setKey(k uint16, value_type value.ValueType, key_value value.Value) {
	from := node.getKeyPosition(k)
	to := from + node.key_length
	v2bytes, _ := key_value.Convert2Bytes()
	copy(node.data[from:to], v2bytes)
}

// Get the end of a node
func (node bpNode) getEnd() uint16 {
	if node.isLeaf() == 1 {
		return node.getPointerPosition(node.getSize())
	} else {
		return node.getKeyPosition(node.getSize())
	}
}

func (node bpNode) getBegin() uint16 {
	return node.getPointerPosition(0)
}

// Get the file name for a certain index
func (info *IndexInfo) getFileName() string {
	return info.Table_name + "_" + info.Attr_name + index_file_suffix
}

// Copy key[src_id] from src into key[des_id] of des
func copyKey(des bpNode, des_id uint16, src bpNode, src_id uint16) {
	key_length := des.key_length
	src_key_pos := src.getKeyPosition(src_id)
	des_key_pos := des.getKeyPosition(des_id)
	copy(des.data[des_key_pos:des_key_pos+key_length], src.data[src_key_pos:src_key_pos+key_length])
}

// Copy P[src_id] from src into P[des_id] of des
func copyPointer(des bpNode, des_id uint16, src bpNode, src_id uint16) {
	var pointer_length uint16
	if des.isLeaf() == 1 {
		pointer_length = 4
	} else {
		pointer_length = 2
	}
	src_pointer_pos := src.getPointerPosition(src_id)
	des_pointer_pos := des.getPointerPosition(des_id)
	copy(des.data[des_pointer_pos:des_pointer_pos+pointer_length], src.data[src_pointer_pos:src_pointer_pos+pointer_length])
}

// Make space for {pointer, key} at position k
func (node bpNode) makeSpace(k uint16) {
	kth_pointer_pos := node.getPointerPosition(k)
	var subnode_length uint16
	if node.isLeaf() == 1 {
		subnode_length = 4 + node.key_length
	} else {
		subnode_length = 2 + node.key_length
	}
	if kth_pointer_pos+subnode_length < BufferManager.BlockSize {
		copy(node.data[kth_pointer_pos+subnode_length:], node.data[kth_pointer_pos:])
	}
}

// Shrink space at position k
func (node bpNode) shrinkSpace(k uint16) {
	kth_pointer_pos := node.getPointerPosition(k)
	var subnode_length uint16
	if node.isLeaf() == 1 {
		subnode_length = 4 + node.key_length
	} else {
		subnode_length = 2 + node.key_length
	}
	if kth_pointer_pos+subnode_length < BufferManager.BlockSize {
		copy(node.data[kth_pointer_pos:], node.data[kth_pointer_pos+subnode_length:])
	}
}

// Split a node into half when it is full
// parent: the parent node
// k: the kth node of the parent is full
// I don't check the error info, that's the job of the buffer manager
func (parent bpNode) splitNode(info IndexInfo, k uint16) {
	filename := info.getFileName()
	key_length := info.Attr_length

	// Get the block id of new node and evil node
	new_node_id, _ := BufferManager.NewBlock(filename)
	evil_node_id := parent.getPointer(k)

	// Get the new node and the evil node
	new_node, new_node_block := getBpNode(filename, new_node_id, key_length)
	new_node_block.SetDirty()
	defer new_node_block.FinishRead()

	evil_node, evil_node_block := getBpNode(filename, evil_node_id, key_length)
	evil_node_block.SetDirty()
	defer evil_node_block.FinishRead()

	M := getOrder(key_length) // The order of the tree

	// Copy half the child into new node
	new_node.setLeaf(evil_node.isLeaf())
	evil_half := evil_node.getPointerPosition((M + 1) / 2)
	new_begin := new_node.getBegin()
	copy(new_node.data[new_begin:], evil_node.data[evil_half:])

	// Deal with parent node
	parent.makeSpace(k) // Make space for the new key & pointer
	if evil_node.isLeaf() == 1 {
		new_node.setSize((M + 1) / 2)
		evil_node.setSize((M + 1) / 2)
		new_node.setNext(evil_node.getNext())
		evil_node.setNext(new_node_id)
		copyKey(parent, k, evil_node, (M+1)/2)
	} else {
		new_node.setSize((M - 1) / 2)
		evil_node.setSize((M - 1) / 2)
		copyKey(parent, k, evil_node, (M-1)/2)
	}
	parent.setPointer(k, parent.getPointer(k+1))
	parent.setPointer(k+1, new_node_id)
	parent.setSize(parent.getSize() + 1)
}

// Merge node k with node k + 1
func (parent bpNode) mergeNode(info IndexInfo, k uint16) {
	filename := info.getFileName()
	key_length := info.Attr_length

	// Get the block id of new node and evil node
	evil_node_id := parent.getPointer(k)
	evil_sib_id := parent.getPointer(k + 1)

	evil_node, evil_node_block := getBpNode(filename, evil_node_id, key_length)
	evil_node_block.SetDirty()
	defer evil_node_block.FinishRead()

	evil_sib, evil_sib_block := getBpNode(filename, evil_sib_id, key_length)
	evil_sib_block.SetDirty()
	defer evil_sib_block.FinishRead()

	evil_node_size := evil_node.getSize()
	evil_sib_size := evil_sib.getSize()

	if evil_node.isLeaf() == 1 {
		evil_node.setNext(evil_sib.getNext())
	}
	evil_node_end := evil_node.getEnd()
	evil_sib_begin := evil_node.getBegin()
	if evil_node.isLeaf() == 0 {
		copyKey(evil_node, evil_node_size, parent, k)
		evil_node_end += key_length
		evil_node.setSize(evil_node_size + evil_sib_size + 1) // trouble
	} else {
		evil_node.setSize(evil_node_size + evil_sib_size)
	}
	copy(evil_node.data[evil_node_end:], evil_sib.data[evil_sib_begin:])
	parent.shrinkSpace(k)
	parent.setPointer(k, evil_node_id)
	parent.setSize(parent.getSize() - 1)
}

// Move the first node of (k + 1) th child to k-th node
func (parent bpNode) moveNode(info IndexInfo, k uint16) {
	filename := info.getFileName()
	key_length := info.Attr_length

	poor_node_id := parent.getPointer(k)
	rich_node_id := parent.getPointer(k + 1)

	poor_node, poor_node_block := getBpNode(filename, poor_node_id, key_length)
	poor_node_block.SetDirty()
	defer poor_node_block.FinishRead()

	rich_node, rich_node_block := getBpNode(filename, rich_node_id, key_length)
	rich_node_block.SetDirty()
	defer rich_node_block.FinishRead()

	var new_key_id, new_pointer_id uint16
	n := poor_node.getSize()
	if poor_node.isLeaf() == 1 {
		new_key_id = n + 1
	} else {
		new_key_id = n
	}
	new_pointer_id = n + 1
	copyKey(poor_node, new_key_id, parent, k)
	copyKey(parent, k, rich_node, 0)
	copyPointer(poor_node, new_pointer_id, rich_node, 0)
	rich_node.shrinkSpace(0)

	rich_node.setSize(rich_node.getSize() - 1)
	poor_node.setSize(poor_node.getSize() + 1)
}

// Move the last node of kth child into (k+1)th child
func (parent bpNode) forwardNode(info IndexInfo, k uint16) {
	filename := info.getFileName()
	key_length := info.Attr_length

	poor_node_id := parent.getPointer(k + 1)
	rich_node_id := parent.getPointer(k)

	var poor_node, rich_node bpNode

	poor_node, poor_node_block := getBpNode(filename, poor_node_id, key_length)
	poor_node_block.SetDirty()
	defer poor_node_block.FinishRead()

	rich_node, rich_node_block := getBpNode(filename, rich_node_id, key_length)
	rich_node_block.SetDirty()
	defer rich_node_block.FinishRead()

	rich_size := rich_node.getSize()

	poor_node.makeSpace(0)
	if rich_node.isLeaf() == 0 {
		copyKey(poor_node, 0, parent, k)
		copyPointer(poor_node, 0, rich_node, rich_size)
	} else {
		copyKey(poor_node, 0, rich_node, rich_size-1)
		copyPointer(poor_node, 0, rich_node, rich_size-1)
	}
	copyKey(parent, k, rich_node, rich_size-1)

	rich_node.setSize(rich_node.getSize() - 1)
	poor_node.setSize(poor_node.getSize() + 1)
}

func (parent bpNode) saveNode(info IndexInfo, k uint16) {
	filename := info.getFileName()
	key_length := parent.key_length
	M := getOrder(key_length)

	if k == 0 { // if this is the last node
		next_node, next_node_block := getBpNode(filename, parent.getPointer(k+1), key_length)
		if next_node.isDanger(M) {
			next_node_block.FinishRead()
			parent.mergeNode(info, k)
		} else {
			next_node_block.FinishRead()
			parent.moveNode(info, k)
		}
	} else {
		prev_node, prev_node_block := getBpNode(filename, parent.getPointer(k-1), key_length)
		if prev_node.isDanger(M) {
			prev_node_block.FinishRead()
			parent.mergeNode(info, k-1)
		} else {
			prev_node_block.FinishRead()
			parent.forwardNode(info, k-1)
		}
	}
	// if k == parent.getSize() { // if this is the last node
	// 	prev_node, prev_node_block := getBpNode(filename, parent.getPointer(k-1), key_length)
	// 	if prev_node.isDanger(M) {
	// 		prev_node_block.FinishRead()
	// 		parent.mergeNode(info, k-1)
	// 	} else {
	// 		prev_node_block.FinishRead()
	// 		parent.forwardNode(info, k-1)
	// 	}
	// } else {
	// 	next_node, next_node_block := getBpNode(filename, parent.getPointer(k+1), key_length)
	// 	if next_node.isDanger(M) {
	// 		next_node_block.FinishRead()
	// 		parent.mergeNode(info, k)
	// 	} else {
	// 		next_node_block.FinishRead()
	// 		parent.moveNode(info, k)
	// 	}
	// }	// This is the origin code
}

func handleRootFull(info IndexInfo) {
	filename := info.getFileName()
	key_length := info.Attr_length

	root, root_block := getBpNode(filename, 0, key_length)
	defer root_block.FinishRead()

	M := getOrder(key_length)

	if root.isFull(M) {
		// If root is full, make it a child of the new node
		root_block.SetDirty()
		new_block_id, _ := BufferManager.NewBlock(filename)
		new_node, new_node_block := getBpNode(filename, new_block_id, key_length)
		new_node_block.SetDirty()

		copy(new_node.data, root.data)
		root.setSize(0)
		root.setLeaf(0)
		root.setPointer(0, new_block_id)

		new_node_block.FinishRead()
	}
}

func handleRootSingle(info IndexInfo) {
	// If root is single, simply copy all the info from the only child into node
	filename := info.getFileName()
	key_length := info.Attr_length

	root, root_block := getBpNode(filename, 0, key_length)
	defer root_block.FinishRead()
	if root.isLeaf() == 0 && root.getSize() == 0 { // Single root
		root_block.SetDirty()
		child, child_block := getBpNode(filename, root.getPointer(0), key_length)
		copy(root.data, child.data)
		child_block.FinishRead()
	}
}
