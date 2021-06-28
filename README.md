# MiniSQL
a simple DBMS for DB course in ZJU with go



we use go mod to manage dependency,  the mod name is minisql





### How To Use?

- create a folder named data at the root position
- `go run main.go`
- enjoy yourself! You can use `test` folder *.txt to run it, also you can fork and update it!





### Modules

- Buffer Manager
- Catalog Manager
- Index Manager
- Record Manager
- Interpreter
- API



Interpreter, Catalog Manager ,API and Buffer Manager is implemented by cxz66666 , Index Manager is implemented by ezoiljy, and Record Manager, front-backend is implemented by Mr_Wolfram



- Interpreter  using lexer and tokenizer, which created by `goyacc`

- Catalog Manager use `msgp` to encode decode

- API and Interpreter  use channel to share data

- Buffer Manager use Mutex.lock to concurrency control, LRU and Two way linked list is used.

  





### TODO：

Front and backend is included in `src/Front` and `src/BackEnd`, but we don't ensure you can run it on you machine( because we don't debug it.... :bug:)
