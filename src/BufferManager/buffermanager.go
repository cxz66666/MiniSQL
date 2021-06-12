package BufferManager

const BlockSize = 4096

func BlockRead(filename string, block_id int) (error, []byte) {

}

func BlockPin(block_id int) (bool, error) {

}

func BlockUnpin(block_id int) (bool, error) {

}

func BlockFlush(block_id int) (bool, error) {

}

func BlockFlushAll() (bool, error) {

}
