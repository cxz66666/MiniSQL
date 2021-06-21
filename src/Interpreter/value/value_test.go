package value

import (
	"testing"
)
var cases1=[]struct{
	left Value
	right Value
	op CompareType

}{
	{left: Int{1}, right: Int{2}, op: 1},
	{left: Int{132432}, right: Int{2234234}, op: 2},
	{left: Int{1324}, right: Int{243543}, op: 3},
	{left: Int{146546}, right: Int{2234324}, op: 4},

}
var cases2=[]struct{
	left Value
	right Value
	op CompareType

}{
	{left: Float{1.3}, right: Float{1.0}, op: 2},
	{left: Float{112.3123}, right: Float{1123214.0}, op: 1},
	{left: Float{11231.3243}, right: Float{12342423.23230}, op: 4},
	{left: Float{1123213.1233}, right: Float{112312.3240}, op: 5},

}
var cases3=[]struct{
	left Value
	right Value
	op CompareType

}{
	{left: Bool{true}, right: Bool{false}, op: 3},
	{left: Bool{true}, right: Bool{true}, op: 3},
	{left: Bool{true}, right: Bool{true}, op: 3},
	{left: Bool{false}, right: Bool{false}, op: 3},

}
var cases4=[]struct{
	left Value
	right Value
	op CompareType

}{
	{left: Bytes{[]byte{1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10}}, right: Bytes{[]byte{1,2,4,5,6,7,8,9,0,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,10}}, op: 2},
	{left: Bytes{[]byte{1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10}}, right: Bytes{[]byte{1,2,4,5,6,7,8,9,0,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,10}}, op: 2},
	{left: Bytes{[]byte{1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10}}, right: Bytes{[]byte{1,2,4,5,6,7,8,9,0,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,10}}, op: 2},
	{left: Bytes{[]byte{1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10}}, right: Bytes{[]byte{1,2,4,5,6,7,8,9,0,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,10}}, op: 2},

}


var cases1_t=[]struct{
	left Int
	right Int
	op CompareType

}{
	{left: Int{1}, right: Int{2}, op: 1},
	{left: Int{132432}, right: Int{2234234}, op: 1},
	{left: Int{1324}, right: Int{243543}, op: 1},
	{left: Int{146546}, right: Int{2234324}, op: 1},

}
var cases2_t=[]struct{
	left Float
	right Float
	op CompareType

}{
	{left: Float{1.3}, right: Float{1.0}, op: 2},
	{left: Float{112.3123}, right: Float{1123214.0}, op: 1},
	{left: Float{11231.3243}, right: Float{12342423.23230}, op: 4},
	{left: Float{1123213.1233}, right: Float{112312.3240}, op: 5},

}
var cases3_t=[]struct{
	left Bool
	right Bool
	op CompareType

}{
	{left: Bool{true}, right: Bool{false}, op: 3},
	{left: Bool{true}, right: Bool{true}, op: 3},
	{left: Bool{true}, right: Bool{true}, op: 3},
	{left: Bool{false}, right: Bool{false}, op: 3},

}
var cases4_t=[]struct{
	left Bytes
	right Bytes
	op CompareType

}{
	{left: Bytes{[]byte{1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10}}, right: Bytes{[]byte{1,2,4,5,6,7,8,9,0,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,10}}, op: 2},
	{left: Bytes{[]byte{1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10}}, right: Bytes{[]byte{1,2,4,5,6,7,8,9,0,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,10}}, op: 2},
	{left: Bytes{[]byte{1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10}}, right: Bytes{[]byte{1,2,4,5,6,7,8,9,0,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,10}}, op: 2},
	{left: Bytes{[]byte{1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10}}, right: Bytes{[]byte{1,2,4,5,6,7,8,9,0,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,1,2,4,5,6,7,8,9,0,10,10,10,10}}, op: 2},


}

func MyCom(i Int, v Int,op CompareType ) ( bool, error ) {
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
	return false,nil
}
func AssertCom(a Value, b Value,op CompareType)(bool,error)  {
	switch a.(type) {
	case Int:
		return a.(Int).Compare(b, op)
	case Float:
		return a.(Float).Compare(b, op)
	case Bytes:
		return a.(Bytes).Compare(b, op)
	}
	return false,nil
}
func Benchmark_Compare(b *testing.B) {

	b.Run("value and value compare without assert give two value", func(b *testing.B) {
		for i := 0; i < b.N; i ++ {
			for _,_case:=range cases1{
				_,err:=_case.left.Compare(_case.right,5)
				if err!=nil{
					b.Error(err)
				}
			}
		}
	})
	b.Run("value and value compare with assert give two value", func(b *testing.B) {
		for i := 0; i < b.N; i ++ {
			for _,_case:=range cases1{
				_,err:=AssertCom(_case.left,_case.right,_case.op)
				if err!=nil{
					b.Error(err)
				}
			}
		}
	})

	b.Run("value and value compare with  give two value and a type", func(b *testing.B) {
		for i := 0; i < b.N; i ++ {
			for _,_case:=range cases1{
				_,err:=CompareWithType(_case.left,_case.right,_case.op,IntType)
				if err!=nil{
					b.Error(err)
				}
			}
		}
	})
	b.Run("two value convert to Int and  compare Int", func(b *testing.B){
		for i := 0; i < b.N; i ++ {
			for _,_case:=range cases1{
				left1:=_case.left.(Int)
				right1:=_case.right.(Int)

				_,err:=left1.Compare(right1,5)
				if err!=nil{
					b.Error(err)
				}
			}
		}
	})

	b.Run("int's compare and give two Int", func(b *testing.B) {

		for i := 0; i < b.N; i ++ {
			for _,_case:=range cases1_t{
				_,err:=_case.left.Compare1(_case.right,5)
				if err!=nil{
					b.Error(err)
				}
			}
		}
	})
	b.Run("native compare  use func(A,B)", func(b *testing.B) {

		for i := 0; i < b.N; i ++ {
			for _,_case:=range cases1_t{
				_,err:=MyCom(_case.left,_case.right,5)
				if err!=nil{
					b.Error(err)
				}
			}
		}
	})

}