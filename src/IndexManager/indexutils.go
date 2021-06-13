package IndexManager

import (
	"bytes"
	"encoding/binary"
	"minisql/src/BufferManager"
	"minisql/src/Interpreter/value"
)

// Get the order of this B+ tree
// Order is supposed to be the maximum *odd* number that
// the block is capable of storing that many keys
func getOrder(key_length uint16) (order uint16) {
	order = (BufferManager.BlockSize-7)/(key_length+4) - 1
	if (order & 1) == 0 {
		order--
	}
	return
}

// Is this node a leaf?
func (node bpNode) isLeaf() uint8 {
	return uint8((node)[0])
}

// Set the leaf property of this node
func (node bpNode) setLeaf(leaf uint8) {
	(node)[0] = byte(leaf)
}

// Get the size of the node
func (node bpNode) getSize() (size uint16) {
	buf := bytes.NewBuffer((node)[1:2])
	binary.Read(buf, binary.LittleEndian, &size)
	return
}

// Set the size of the node
func (node bpNode) setSize(size uint16) {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, size)
	copy((node)[1:2], buf.Bytes())
}

// Get the start of the P[k]
func (node bpNode) getPointerPosition(key_length uint16, k uint16) (offset uint16) {
	if node.isLeaf() == 1 {
		subnode_length := key_length + 4
		offset = 7 + k*subnode_length
		return
	} else {
		subnode_length := key_length + 2
		offset = 3 + k*subnode_length
		return
	}
}

// Get the start of the Key[k]
func (node bpNode) getKeyPosition(key_length uint16, k uint16) (offset uint16) {
	if node.isLeaf() == 1 {
		subnode_length := key_length + 4
		offset = 11 + k*subnode_length
		return
	} else {
		subnode_length := key_length + 2
		offset = 5 + k*subnode_length
		return
	}
}

// Get P[k] (for internal node)
func (node bpNode) getPointer(key_length uint16, k uint16) (block_id uint16) {
	from := node.getPointerPosition(key_length, k)
	to := from + 1
	buf := bytes.NewBuffer((node)[from:to])
	binary.Read(buf, binary.LittleEndian, &block_id)
	return
}

// Set P[k]
func (node bpNode) setPointer(key_length uint16, k uint16, block_id uint16) {
	from := node.getPointerPosition(key_length, k)
	to := from + 1
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, block_id)
	copy(node[from:to], buf.Bytes())
}

// Get P[k] (for leaf node)
func (node bpNode) getFilePointer(key_length uint16, k uint16) (pos Position) {
	from := node.getPointerPosition(key_length, k)
	to := from + 1
	buf := bytes.NewBuffer((node)[from:to])
	binary.Read(buf, binary.LittleEndian, &pos.block)

	// Get Offset
	from += 2
	to += 2
	buf = bytes.NewBuffer((node)[from:to])
	binary.Read(buf, binary.LittleEndian, &pos.offset)
	return
}

// Set P[k] (for leaf node)
func (node bpNode) setFilePointer(key_length uint16, k uint16, pos Position) {
	from := node.getPointerPosition(key_length, k)
	to := from + 1
	buf := bytes.NewBuffer(node[from:to])
	binary.Write(buf, binary.LittleEndian, pos.block)

	// Get Offset
	from += 2
	to += 2
	buf = bytes.NewBuffer(node[from:to])
	binary.Write(buf, binary.LittleEndian, pos.offset)
	return
}

// Get previous leaf
func (node bpNode) getPrev() (block_id uint16) {
	buf := bytes.NewBuffer((node)[3:4])
	binary.Read(buf, binary.LittleEndian, &block_id)
	return
}

// Set prev
func (node bpNode) setPrev(block_id uint16) {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, block_id)
	copy(node[3:4], buf.Bytes())
	return
}

// Get next leaf
func (node bpNode) getNext() (block_id uint16) {
	buf := bytes.NewBuffer((node)[5:6])
	binary.Read(buf, binary.LittleEndian, &block_id)
	return
}

// Set Next
func (node bpNode) setNext(block_id uint16) {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, block_id)
	copy(node[5:6], buf.Bytes())
	return
}

// Get Key[k]
func (node bpNode) getKey(key_length uint16, value_type value.ValueType, k uint16) value.Value {
	from := node.getKeyPosition(key_length, k)
	to := from + key_length - 1
	val, err := value.Byte2Value((node)[from:to], value_type, int(key_length))
	if err != nil {
		panic(err)
	} else {
		return val
	}
}

// Set Key[k]
func (node bpNode) setKey(key_length uint16, k uint16, value_type value.ValueType, key_value value.Value) {
	from := node.getKeyPosition(key_length, k)
	to := from + key_length - 1
	v2bytes, _ := key_value.Convert2Bytes()
	copy(node[from:to], v2bytes)
}

// Get the end of a node

func (node bpNode) getEnd(key_length uint16) uint16 {
	if node.isLeaf() == 1 {
		return node.getKeyPosition(key_length, node.getSize()) + key_length
	} else {
		return node.getPointerPosition(key_length, node.getSize()) + 2
	}
}

// Get the file name for a certain index
func (info *IndexInfo) getFileName() string {
	return info.Table_name + "_" + info.Attr_name + index_file_suffix
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
	evil_node_id := parent.getPointer(info.Attr_length, k)

	// Get the new node and the evil node
	var new_node, evil_node bpNode
	new_node, _ = BufferManager.BlockWrite(filename, new_node_id)
	BufferManager.BlockPin(new_node_id)
	defer BufferManager.BlockUnpin(new_node_id)
	evil_node, _ = BufferManager.BlockWrite(filename, uint16(evil_node_id))
	BufferManager.BlockPin(uint16(evil_node_id))
	defer BufferManager.BlockUnpin(uint16(evil_node_id))

	M := getOrder(info.Attr_length) // The order of the tree

	new_node.setSize((M - 1) / 2)
	new_node.setLeaf(evil_node.isLeaf())
	evil_half := evil_node.getPointerPosition(key_length, ((M + 1) / 2))
	new_begin := new_node.getPointerPosition(key_length, 0)
	var subnode_length uint16

	if evil_node.isLeaf() == 1 { // If this is a leaf
		subnode_length = key_length + 4
		evil_node.setSize((M + 1) / 2)
		new_node.setPrev(evil_node_id)
		new_node.setNext(evil_node.getNext())
	} else {
		subnode_length = key_length + 2
		evil_node.setSize((M - 1) / 2)

	}
	copy(new_node[new_begin:], evil_node[evil_half:])

	// Deal with parent node
	parent.setSize(parent.getSize() + 1)
	kth_key_pos := parent.getKeyPosition(key_length, k)             // Position of the kth key
	copy(parent[kth_key_pos+subnode_length:], parent[kth_key_pos:]) // Make space for the new node
	mid_key := evil_node.getKeyPosition(key_length, (M-1)/2)        // The medium key in the evil node
	copy(parent[kth_key_pos:kth_key_pos+key_length-1], evil_node[mid_key:mid_key+key_length-1])
	parent.setPointer(key_length, k, new_node_id)
}

// Merge node k with node k + 1
func (parent bpNode) mergeNode(info IndexInfo, k uint16) {
	filename := info.getFileName()
	key_length := info.Attr_length

	// Get the block id of new node and evil node
	evil_node_id := parent.getPointer(key_length, k)
	evil_sib_id := parent.getPointer(key_length, k+1)

	var evil_node, evil_sib bpNode

	evil_node, _ = BufferManager.BlockWrite(filename, evil_node_id)
	BufferManager.BlockPin(evil_node_id)
	defer BufferManager.BlockUnpin(evil_node_id)
	evil_sib, _ = BufferManager.BlockWrite(filename, evil_sib_id)
	BufferManager.BlockPin(evil_sib_id)
	defer BufferManager.BlockUnpin(evil_sib_id)

	evil_node.setSize(evil_node.getSize() + evil_sib.getSize())

	if evil_node.isLeaf() == 1 {
		evil_node.setNext(evil_sib.getNext())
	}
	evil_node_end := evil_node.getEnd(key_length)
	evil_sib_begin := evil_node.getPointerPosition(key_length, 0)
	kth_key_pos := parent.getKeyPosition(key_length, k)
	if evil_node.isLeaf() == 0 {
		copy(evil_node[evil_node_end:evil_node_end+key_length-1], parent[kth_key_pos:kth_key_pos+key_length-1])
		evil_node_end += key_length
	}
	copy(evil_node[evil_node_end:], evil_sib[evil_sib_begin:])
	copy(parent[kth_key_pos:], parent[kth_key_pos+key_length+2:])
}

// Move the first node of (k + 1) th child to k-th node
func (parent bpNode) moveNode(info IndexInfo, k uint16) {
	filename := info.getFileName()
	key_length := info.Attr_length

	poor_node_id := parent.getPointer(key_length, k)
	rich_node_id := parent.getPointer(key_length, k+1)

	var poor_node, rich_node bpNode
	poor_node, _ = BufferManager.BlockWrite(filename, poor_node_id)
	BufferManager.BlockPin(poor_node_id)
	defer BufferManager.BlockUnpin(poor_node_id)
	rich_node, _ = BufferManager.BlockWrite(filename, rich_node_id)
	BufferManager.BlockPin(rich_node_id)
	defer BufferManager.BlockUnpin(rich_node_id)

	n := poor_node.getSize()
	kth_key_pos := parent.getKeyPosition(key_length, k)
	src_key_pos := rich_node.getKeyPosition(key_length, 0)
	src_pointer_pos := rich_node.getPointerPosition(key_length, 0)
	var des_pointer_pos, des_key_pos, pointer_length uint16
	if poor_node.isLeaf() == 1 {
		des_key_pos = poor_node.getKeyPosition(key_length, n+1)
		des_pointer_pos = poor_node.getPointerPosition(key_length, n+1)
		pointer_length = 4
	} else {
		des_key_pos = poor_node.getKeyPosition(key_length, n)
		des_pointer_pos = poor_node.getPointerPosition(key_length, n+1)
		pointer_length = 2
	}
	copy(poor_node[des_key_pos:des_key_pos+key_length-1], parent[kth_key_pos:kth_key_pos+key_length-1])
	copy(parent[kth_key_pos:kth_key_pos+key_length-1], rich_node[src_key_pos:src_key_pos+key_length-1])
	copy(poor_node[des_pointer_pos:des_pointer_pos+pointer_length-1], rich_node[src_pointer_pos:src_pointer_pos+pointer_length-1])
	copy(rich_node[src_pointer_pos:], rich_node[src_pointer_pos+key_length+pointer_length:])

	rich_node.setSize(rich_node.getSize() - 1)
	poor_node.setSize(poor_node.getSize() + 1)
}
