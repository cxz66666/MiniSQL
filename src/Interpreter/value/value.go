package value



import (
	"bytes"
	"encoding/binary"
	"fmt"
)
//go:generate msgp

type CompareType int
const (
	Great CompareType=iota
	GreatEqual
	Less
	LessEqual
	Equal
	NotEqual
)
type ValueType=int
const (
	BoolType ValueType = iota
	IntType
	FloatType
	StringType
	BytesType
	DateType
	TimestampType
	NullType
	AlienType
)
type Value interface {
	String()string
	Compare(Value, CompareType)(bool,error)
	SafeCompare(Value,CompareType)(bool,error)
	Convert2Bytes() ([]byte,error)
	Convert2IntType()(int)

}
type Int struct {
	Val int64
}
type Float struct {
	Val float64
}
type Bytes struct {
	Val []byte
}
type Bool struct {
	Val bool
}
type Null struct{
	length int
}
type Alien struct {
	Val interface{}
}


func (i Int)String() string {
	return fmt.Sprint(i.Val)
}
func (i Int)Compare1(v Int,op CompareType)(bool,error) {
	switch op {
	case Great:
		return i.Val > v.Val,nil
	case GreatEqual:
		return i.Val >= v.Val,nil
	case Less:
		return i.Val < v.Val,nil
	case LessEqual:
		return i.Val <= v.Val,nil
	case Equal:
		return i.Val == v.Val,nil
	case NotEqual:
		return i.Val != v.Val,nil
	}
	return false,fmt.Errorf("unknow operation type %d", op)
}
func (i Int)Compare(v Value,op CompareType)(bool,error) {
	switch op {
	case Great:
		return i.Val > v.(Int).Val,nil
	case GreatEqual:
		return i.Val >= v.(Int).Val,nil
	case Less:
		return i.Val < v.(Int).Val,nil
	case LessEqual:
		return i.Val <= v.(Int).Val,nil
	case Equal:
		return i.Val == v.(Int).Val,nil
	case NotEqual:
		return i.Val != v.(Int).Val,nil
	}
	return false,fmt.Errorf("unknow operation type %d", op)
}
func (i Int)SafeCompare(v Value,op CompareType)(bool,error) {
	switch v.(type) {
	case Int:
		return i.Compare(v,op)
	case Float:
		var tmp_i =Float{Val: float64(i.Val)}
		return tmp_i.Compare(v,op)
	default:
		return false,nil
	}
	return false,nil
}
func (i Int)Convert2Bytes() ([]byte,error) {
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.LittleEndian, i.Val)
	return bytebuf.Bytes(),nil
}
func (i Int)Convert2IntType() int {
	return IntType
}

func (i Float) String() string {
	return fmt.Sprint(i.Val)
}
func (i Float)Compare(v Value,op CompareType)(bool,error) {
	switch op {
	case Great:
		return i.Val > v.(Float).Val,nil
	case GreatEqual:
		return i.Val >= v.(Float).Val,nil
	case Less:
		return i.Val < v.(Float).Val,nil
	case LessEqual:
		return i.Val <= v.(Float).Val,nil
	case Equal:
		return i.Val == v.(Float).Val,nil
	case NotEqual:
		return i.Val != v.(Float).Val,nil
	}
	return false,fmt.Errorf("unknow operation type %d", op)
}
func (i Float)SafeCompare(v Value,op CompareType)(bool,error) {
	switch v.(type) {
	case Float:
		return i.Compare(v,op)
	case Int:
		var tmp_v =Float{Val: float64(v.(Int).Val)}
		return i.Compare(tmp_v,op)
	default:
		return false,nil
	}
	return false,nil
}
func (i Float)Convert2Bytes() ([]byte,error) {
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.LittleEndian, i.Val)
	return bytebuf.Bytes(),nil
}
func (i Float)Convert2IntType() int {
	return FloatType
}

func (i Bytes) String() string {
	var ans []byte
	for _,v:=range i.Val{
		if v==0{
			break
		}
		ans=append(ans,v)
	}
	return string(ans)
}

func (i Bytes)Compare(v Value,op CompareType)(bool,error) {
	v_val:=(v).(Bytes).Val[:]
	v_len:=len(v_val)
	i_len:=len(i.Val)
	switch op {
	case Great:
		for index,item:=range i.Val{
			if index+ 1<=v_len&&item>v_val[index]{
				return true,nil
			} else if  index+ 1<=v_len&&item<v_val[index] {
				return false,nil
			} else if index+1>v_len{
				return true,nil
			}
		}
		if v_len==i_len{
			return false,nil
		}
		return false,nil
	case GreatEqual:

		for index,item:=range i.Val{
			if index+ 1<=v_len&&item>v_val[index]{
				return true,nil
			} else if  index+ 1<=v_len&&item<v_val[index] {
				return false,nil
			} else if index+1>v_len{
				return true,nil
			}
		}
		if i_len==v_len{
			return true,nil
		}
		return false,nil
	case Less:
		for index,item:=range i.Val{
			if index+ 1<=v_len&&item<v_val[index]{
				return true,nil
			} else if  index+ 1<=v_len&&item>v_val[index] {
				return false,nil
			} else if index+1>v_len{
				return false,nil
			}
		}
		if i_len==v_len{
			return false,nil
		}
		return false,nil
	case LessEqual:
		for index,item:=range i.Val{
			if index+ 1<=v_len&&item<v_val[index]{
				return true,nil
			} else if  index+ 1<=v_len&&item>v_val[index] {
				return false,nil
			} else if index+1>v_len{
				return false,nil
			}
		}
		if i_len==v_len{
			return true,nil
		}
		return false,nil
	case Equal:
		if v_len!=i_len {
			return  false,nil
		}
		for index,item:=range i.Val{
			if index+ 1<=v_len&&item!=v_val[index]{
				return false,nil
			} else if index+1>v_len{
				return false,nil
			}
		}
		return true,nil
	case NotEqual:
		for index,item:=range i.Val{
			if index+ 1<=v_len&&item!=v_val[index]{
				return true,nil
			} else if index+1>v_len{
				return true,nil
			}
		}
		if(i_len==v_len){
			return false,nil
		}
		return true,nil
	}
	return false,fmt.Errorf("unknow operation type %d", op)
}
func (i Bytes)SafeCompare(v Value,op CompareType)(bool,error) {
	if _,ok:=v.(Bytes);ok{
		return i.Compare(v,op)
	}
	return false,nil
}
func (i Bytes)Convert2Bytes() ([]byte,error) {
	return i.Val,nil
}
func (i Bytes)Convert2IntType() int {
	return BytesType
}

func (i Bool) String() string {
	return fmt.Sprint(i.Val)
}
func (i Bool)Compare(v Value,op CompareType)(bool,error) {
	switch op {
	case Great:
		return false,nil
	case GreatEqual:
		return false,nil
	case Less:
		return false,nil
	case LessEqual:
		return false,nil
	case Equal:
		return i.Val == v.(Bool).Val,nil
	case NotEqual:
		return i.Val != v.(Bool).Val,nil
	}
	return false,fmt.Errorf("unknow operation type %d", op)
}
func (i Bool)SafeCompare(v Value,op CompareType)(bool,error) {
	if _,ok:=v.(Bool);ok{
		return i.Compare(v,op)
	}
	return false,nil
}
func (i Bool)Convert2Bytes() ([]byte,error) {
	if i.Val {
		return []byte{1},nil
	}
	return []byte{0},nil
}
func (i Bool)Convert2IntType() int {
	return BoolType
}

func (i Alien) String() string  {
	return fmt.Sprint(i.Val)
}
func (i Alien)Compare(v Value,op CompareType)(bool,error) {
	switch op {
	case Great:
		return false,nil
	case GreatEqual:
		return false,nil
	case Less:
		return false,nil
	case LessEqual:
		return false,nil
	case Equal:
		return false,nil
	case NotEqual:
		return false,nil
	}
	return false,fmt.Errorf("unknow operation type %d", op)
}
func (i Alien)SafeCompare(v Value,op CompareType)(bool,error) {
	if _,ok:=v.(Alien);ok{
		return i.Compare(v,op)
	}
	return false,nil
}
func (i Alien)Convert2Bytes() ([]byte,error) {
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.LittleEndian, i.Val)
	return bytebuf.Bytes(),nil
}
func (i Alien)Convert2IntType() int {
	return  AlienType
}

func (i Null) String() string  {
	return "null"
}
func (i Null)Compare(v Value,op CompareType)(bool,error) {
	if op==Equal {
		return true,nil
	}
	return false,nil
}
func (i Null)SafeCompare(v Value,op CompareType)(bool,error) {
	if _,ok:=v.(Null);ok{
		return i.Compare(v,op)
	}
	return false,nil
}
func (i Null)Convert2Bytes() ([]byte,error) {
	return make([]byte,i.length),nil
}
func (i Null)Convert2IntType() int {
	return NullType
}
func NewFromParquetValue(v interface{}) Value {
	switch v.(type) {
	case int:
		return Int{Val: int64(v.(int))}
	case float64:
		return Float{Val: v.(float64)}
	case []byte:
		return Bytes{Val: v.([]byte)}
	case int8:
		return Int{Val: int64(v.(int8))}
	case int16:
		return Int{Val: int64(v.(int16))}
	case int32:
		return Int{Val: int64(v.(int32))}
	case int64:
		return Int{Val: v.(int64)}
	case uint:
		return Int{Val: int64(v.(uint))}
	case uint8:
		return Int{Val: int64(v.(uint8))}
	case uint16:
		return Int{Val: int64(v.(uint16))}
	case uint32:
		return Int{Val: int64(v.(uint32))}
	case uint64:
		return Int{Val: int64(v.(uint64))}
	case float32:
		return Float{Val: float64(v.(float32))}

	case bool:
		return Bool{Val: v.(bool)}

	default:
		return Alien{Val: v}
	}
}
func CompareWithType(i Value,v Value,op CompareType) (bool,error) {
	return i.(Int).Compare(v,op)
}
