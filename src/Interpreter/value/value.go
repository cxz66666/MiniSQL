package value



import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"minisql/src/Utils"
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
type Row struct {  // It's a row for record
	Values []Value
}
//Value is the most important type!
type Value interface {
	String()string
	Compare(Value, CompareType)(bool,error)
	SafeCompare(Value,CompareType)(bool,error)
	Convert2Bytes() ([]byte,error)
	Convert2IntType() ValueType
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

	i_str:=Utils.CString(i.Val)
	v_str:=Utils.CString((v).(Bytes).Val)
	switch op {
	case Great:
		return i_str>v_str,nil
	case GreatEqual:
		return i_str>=v_str,nil
	case Less:

		return i_str<v_str,nil
	case LessEqual:

		return i_str<=v_str,nil
	case Equal:
		return i_str==v_str,nil
	case NotEqual:
		return i_str!=v_str,nil
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

// NewFromParquetValue you can input a arbitrary type into it, and it will try it's best to convert it to Value
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

//Byte2Value convert byte to Value ,length is used for char
// IntType,FloatType,BoolType don't need a length, so you can use this function like Byte2Value([]bytes,IntType)
// BytesType and NullType need a length to insure it's correct, so you must use this function like  Byte2Value([]bytes,BytesType,10)
func Byte2Value(mybytes []byte,vt ValueType,length ...int) (Value,error)  {
	switch vt {
	case BoolType:
		if len(mybytes)<1 {
			return nil,errors.New("mybytes length is less than 1")
		}
		if mybytes[0]==1 {
			return Bool{Val: true},nil
		} else if mybytes[0]==0 {
			return Bool{Val: false},nil
		}
		return nil,errors.New("this byte is not a bool byte")
	case IntType:
		if len(mybytes)<8 {
			return nil,errors.New("mybytes length is less than 8")
		}
		var ret int64
		buf:=bytes.NewBuffer(mybytes[0:8])
		binary.Read(buf, binary.LittleEndian, &ret)
		return Int{Val: ret},nil
	case FloatType:
		if len(mybytes)<8{
			return nil,errors.New("mybytes length is less than 8")
		}
		var ret float64
		buf:=bytes.NewBuffer(mybytes[0:8])
		binary.Read(buf,binary.LittleEndian,&ret)
		return Float{Val: ret},nil
	case BytesType:
		if len(length)<1 ||length[0]<=0{
			return nil,errors.New("please input a length for bytes")
		}
		if len(mybytes)<length[0] {
			return nil,errors.New("bytes don't have enough length to convert to bytes")
		}
		ret:=make([]byte,0,length[0]+1)
		ret=append(ret,mybytes[0:length[0]]...)
		return Bytes{Val: ret},nil
	case NullType:
		if len(length)<1 ||length[0]<=0{
			return nil,errors.New("please input a length for bytes")
		}
		if len(mybytes)<length[0] {
			return nil,errors.New("bytes don't have enough length to convert to bytes")
		}
		return Null{length:length[0]},nil
	}
	return nil,errors.New("The type is not supported.")
}
//
func CompareWithType(i Value,v Value,op CompareType,vt ValueType) (bool,error) {
	switch vt {
	case BoolType:
		return i.(Bool).Compare(v,op)
	case IntType:
		return i.(Int).Compare(v,op)
	case FloatType:
		return i.(Float).Compare(v,op)
	case BytesType:
		return i.(Bytes).Compare(v,op)
	case NullType:
		return i.(Null).Compare(v,op)
	case AlienType:
		return i.(Alien).Compare(v,op)
	}
	return false,errors.New("The type is not supported.")
}
