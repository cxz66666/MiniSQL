package parser

var sql_strings=[]string{
	"create table cxz(" +
		"afsdfsad int unique," +
		"what char(30) not null," +
		"primary key (what)" +
		");",
		"select a,b,c,d,e,f,g from cxz where a=123 and b=456 or c=234;",
}

