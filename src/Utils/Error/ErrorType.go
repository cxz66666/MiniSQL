package Error

import "minisql/src/Interpreter/value"

type Error struct {
	Status bool  //返回状态
	Rows   int  //影响的行数
	Data   []value.Row  //返回的数据 这里还没有转成string
	ErrorHint error  //错误提示
}

func CreateFailError(e error) Error  {
	return Error{
		Status: false,
		ErrorHint: e,
	}
}
func CreateSuccessError()  Error {
	return Error{
		Status: true,
		ErrorHint: nil,
	}
}
func CreateRowsError(rows int) Error  {
	return Error{
		Status: true,
		Rows: rows,
		ErrorHint: nil,
	}
}
func CreateDataError(rows int,data []value.Row) Error {
	return Error{
		Status: true,
		Rows: rows,
		Data: data,
		ErrorHint: nil,
	}
}