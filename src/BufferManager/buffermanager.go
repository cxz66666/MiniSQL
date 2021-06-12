package BufferManager

const BlockSize = 4096

func BlockRead(filename string, block_id int) ([]byte, error) {

}

// 返回的 block id 是指新的块在文件中的 block id
func NewBlock(filename string) (block_id int, err error) {

}

func BlockPin(block_id int) (bool, error) {

}

func BlockUnpin(block_id int) (bool, error) {

}

func BlockFlush(block_id int) (bool, error) {

}

func BlockFlushAll() (bool, error) {

}
