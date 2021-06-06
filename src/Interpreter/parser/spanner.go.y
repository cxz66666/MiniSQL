%{
package parser

import (
    "strconv"
	"minisql/src/Interpreter/value"
	"minisql/src/Interpreter/types"
)
%}

%union {
  empty     struct{}
  flag      bool
  i64       int64
  int       int
  f64       float64
  str       string
  strs      []string
  col       types.Column
  cols      []types.Column
  coltype   types.ColumnType
  key       types.Key
  keys      []types.Key
  keyorder  types.KeyOrder
  clstr     types.Cluster
  ondelete  types.OnDelete
  stcls     types.StoringClause
  intlr     types.Interleave
  intlrs    []types.Interleave
  fieldsname types.FieldsName
  LastToken int
  expr   types.Expr
  where     *types.Where
  limit     types.Limit
  compare   value.CompareType
  valuetype value.Value
  valuetypelist []value.Value
  setexpr    types.SetExpr
  setexprlist []types.SetExpr
}
%token<str> IDENT IDENT_LEGAL
%token<str> PRIMARY KEY ASC DESC
%token<str> IN
%token<str>   INTERLEAVE
%token<str> AND OR  NOT NULL
%token<str> ON  CASCADE NO ACTION
%token<str> MAX UNIQUE
%token<str> ADD COLUMN SET
%token<str> TRUE FALSE allow_commit_timestamp
%token<empty> '(' ',' ')' ';' '*'
%left <str> '='  '<' '>' LE GE NE
%token<str> CREATE  DROP
%token<str> DATABASE TABLE INDEX STORING
%token<str> SELECT WHERE FROM LIMIT OFFSET VALUES
%token<str> INSERT INTO UPDATE DELETE
%token<str> BOOL INT64 FLOAT64 STRING BYTES DATE TIMESTAMP

%token<str> database_id
%token<str> table_name
%token<str> column_name
%token<str> index_name

%type<col> column_def
%type<cols> column_def_list
%type<coltype> column_type scalar_type
%type<int> length
%type<i64> int64_value
%type<f64> float64_value
%token<str> decimal_value hex_value float_value string_value

%type<keyorder> key_order_opt
%type<key> key_part
%type<keys> key_part_list
%type<keys> primary_key

%type<clstr> cluster_opt
%type<clstr> cluster
%type<ondelete> on_delete_opt

%type<flag> not_null_opt
%type<flag> unique_opt
%type<str> IDENT_ALL
%type<strs> column_name_list table_name_list
%type<stcls> storing_clause storing_clause_opt
%type<intlr> interleave_clause
%type<intlrs> interleave_clause_list
%type<fieldsname> sel_field_list
%type<expr>  expr_opt
%type<limit> limit_opt
%type<compare> compare_type
%type<valuetype> value
%type<int> int_value
%type<where> where_opt
%type<setexpr> set_opt
%type<setexprlist> set_opt_list
%type<valuetypelist>value_list

%start statements


%%

statements:
    statement
  | statements statement

statement:
    create_database ';'
  | create_table ';'
  | create_index ';'
  | drop_database ';'
  | drop_table ';'
  | drop_index ';'
  | select_stmt ';'
  | insert_stmt ';'
  | update_stmt ';'
  | delete_stmt ';'
create_database:
  CREATE DATABASE database_id
  {
    s := types.CreateDatabaseStatement{
      DatabaseId: $3,
    }
    yylex.(*lexerWrapper).result = append(yylex.(*lexerWrapper).result, s)
  }

create_table:
  CREATE TABLE table_name '(' column_def_list primary_key ')'  cluster_opt
  {
    s := types.CreateTableStatement{
      TableName:   $3,
      Columns:     $5,
      PrimaryKeys: $6,
      Cluster:     $8,
    }
    yylex.(*lexerWrapper).result = append(yylex.(*lexerWrapper).result, s)
  }

column_def_list:
  /* empty */
  {
    $$ = make([]types.Column, 0, 0)
  }
  | column_def
  {
    $$ = make([]types.Column, 0, 1)
    $$ = append($$, $1)
  }
  | column_def ',' column_def_list
  {
    $$ = append($3, $1)
  }

column_def:
  IDENT_LEGAL column_type unique_opt not_null_opt
  {
    $$ = types.Column{Name: $1, Type: $2,Unique:$3,NotNull: $4}
  }

primary_key:
  {
    $$= make([]types.Key, 0, 1)
  }
  | PRIMARY KEY '(' key_part_list ')'
  {
    $$ = $4
  }

key_part_list:
    key_part
  {
    $$ = make([]types.Key, 0, 1)
    $$ = append($$, $1)
  }
  | key_part_list ',' key_part
  {
    $$ = append($1, $3)
  }

key_part:
  IDENT_LEGAL key_order_opt
  {
    $$ = types.Key{Name: $1, KeyOrder: $2}
  }

key_order_opt:
  /* empty */
  {
    $$ = types.Asc
  }
  | ASC
  {
    $$ = types.Asc
  }
  | DESC
  {
    $$ = types.Desc
  }

cluster_opt:
  /* empty */
  {
    $$ = types.Cluster{}
  }
  | ',' cluster
  {
    $$ = $2
  }

cluster:
     on_delete_opt
  {
    $$ = types.Cluster{OnDelete: $1}
  }

on_delete_opt:
  /* empty */
  {
    // default
    $$ = types.NoAction
  }
  | ON DELETE CASCADE
  {
    $$ = types.Cascade
  }
  | ON DELETE NO ACTION
  {
    $$ = types.NoAction
  }

column_type:
    scalar_type
  {
    $$ = $1
  }


scalar_type:
    BOOL
  {
    $$ = types.ColumnType{TypeTag: types.Bool}
  }
  | INT64
  {
    $$ = types.ColumnType{TypeTag: types.Int64}
  }
  | FLOAT64
  {
    $$ = types.ColumnType{TypeTag: types.Float64}
  }

  | BYTES '(' length ')'
  {
    $$ = types.ColumnType{TypeTag: types.Bytes, Length: $3}
  }
  | DATE
  {
    $$ = types.ColumnType{TypeTag: types.Date}
  }
  | TIMESTAMP
  {
    $$ = types.ColumnType{TypeTag: types.Timestamp}
  }

length:
    int_value
  {
    $$ = $1
  }
  | MAX
  {
    $$ = 255
  }



//options_def:
//  /* empty */
//  {
//    $$ = ""
//  }
//  | OPTIONS '(' allow_commit_timestamp '=' TRUE ')'
//  {
//    $$ = $3 + "=" + $5
//  }
//  | OPTIONS '(' allow_commit_timestamp '=' NULL ')'
//  {
//    $$ = $3 + "=" + $5
//  }

not_null_opt:
  /* empty */
  {
    $$ = types.False
  }
  | NOT NULL
  {
    $$ = types.True
  }

create_index:
  CREATE unique_opt  INDEX index_name ON table_name '(' key_part_list ')' storing_clause_opt interleave_clause_list
  {
    s := types.CreateIndexStatement{
      Unique:        $2,
      IndexName:     $4,
      TableName:     $6,
      Keys:          $8,
      StoringClause: $10,
      Interleaves:   $11,
    }
    yylex.(*lexerWrapper).result = append(yylex.(*lexerWrapper).result, s)
  }

unique_opt:
  /* empty */
  {
    $$ = types.False
  }
  | UNIQUE
  {
    $$ = types.True
  }


storing_clause_opt:
  /* empty */
  {
    $$ = types.StoringClause{}
  }
  | storing_clause
  {
    $$ = $1
  }

storing_clause:
    STORING '(' column_name_list ')'
  {
    $$ = types.StoringClause{ColumnNames: $3}
  }

column_name_list:
    IDENT_LEGAL
  {
    $$ = make([]string, 0, 1)
    $$ = append($$, $1)
  }
  | column_name_list ',' IDENT_LEGAL
  {
    $$ = append($1, $3)
  }

interleave_clause_list:
  /* empty */
  {
    $$ = make([]types.Interleave, 0, 0)
  }
  | interleave_clause
  {
    $$ = make([]types.Interleave, 0, 1)
    $$ = append($$, $1)
  }
  | interleave_clause_list ',' interleave_clause
  {
    $$ = append($1, $3)
  }

interleave_clause:
    INTERLEAVE IN IDENT_ALL
  {
    $$ = types.Interleave{TableName: $3}
  }

 drop_database:
  DROP DATABASE database_id
  {
    s := types.DropDatabaseStatement{
      DatabaseId: $3,
    }
    yylex.(*lexerWrapper).result = append(yylex.(*lexerWrapper).result, s)
  }

drop_table:
    DROP TABLE table_name
  {
    s := types.DropTableStatement{
      TableName: $3,
    }
    yylex.(*lexerWrapper).result = append(yylex.(*lexerWrapper).result, s)
  }

drop_index:
    DROP INDEX index_name
  {
    s := types.DropIndexStatement{
      IndexName: $3,
    }
    yylex.(*lexerWrapper).result = append(yylex.(*lexerWrapper).result, s)
  }

insert_stmt:
    INSERT INTO table_name VALUES '(' value_list ')'
    {
      s:=types.InsertStament{
      	TableName: $3,
      	ColumnNames: make([]string, 0, 0),
      	Values: $6,
      }
      yylex.(*lexerWrapper).result = append(yylex.(*lexerWrapper).result, s)
    }
    | INSERT INTO table_name '(' column_name_list ')' VALUES '(' value_list ')'
    {
      s:=types.InsertStament{
      	TableName: $3,
      	ColumnNames: $5,
      	Values: $9,
      }
      yylex.(*lexerWrapper).result = append(yylex.(*lexerWrapper).result, s)
    }
update_stmt:
    UPDATE table_name SET  set_opt_list  where_opt
    {
      s:=types.UpdateStament{
      	TableName: $2,
      	SetExpr: $4,
      	Where: $5,
      }
      yylex.(*lexerWrapper).result = append(yylex.(*lexerWrapper).result, s)
    }
set_opt_list:
  set_opt
  {
    $$ = make([]types.SetExpr, 0, 1)
    $$ = append($$, $1)
  }
  | set_opt_list ',' set_opt
  {
    $$ = append($1, $3)
  }
set_opt:
  IDENT_LEGAL '=' value
  {
    $$=types.SetExpr{
    	Left: $1,
    	Right: $3,
    }
  }
delete_stmt:
    DELETE FROM IDENT_ALL where_opt
    {
      s:=types.DeleteStatement{
      	TableName: $3,
      	Where: $4,
      }
      yylex.(*lexerWrapper).result = append(yylex.(*lexerWrapper).result, s)
    }
select_stmt:
    SELECT sel_field_list FROM table_name_list where_opt limit_opt
    {
      s:=types.SelectStatement{
      	Fields: $2,
      	TableNames: $4,
      	Where: $5,
      	Limit: $6,
      }
      yylex.(*lexerWrapper).result = append(yylex.(*lexerWrapper).result, s)
    }
sel_field_list:
   '*'
   {
     $$=types.FieldsName{
     	SelectAll:true,
     }
   }
   | column_name_list
   {
     $$=types.FieldsName{
     	SelectAll:false,
     	ColumnNames:$1,
     }
   }

table_name_list: // TODO  mulitplart where condition, now only one table can be select
    IDENT_ALL  //here we use table_name which is a string type ,not INDENT
    {
      $$ = make([]string, 0, 1)
      $$ = append($$, $1)
    }
    | table_name_list ',' IDENT_ALL
    {
      $$ =append($1, $3)
    }

where_opt:
    {
      $$=nil
    }
    | WHERE expr_opt
    {
      $$=  &types.Where{Expr:$2}
    }
expr_opt:
    '(' expr_opt ')'
    {
    	$$=$2
    }
    | IDENT_ALL compare_type value
    {
	$$= &types.ComparisonExprLSRV{Left: $1,Operator:$2, Right:$3 }
    }
    |  value compare_type IDENT_ALL
    {
	$$= &types.ComparisonExprLVRS{Left: $1,Operator:$2, Right:$3 }
    }
    |  value compare_type value
    {
	$$= &types.ComparisonExprLVRV{Left: $1,Operator:$2, Right:$3 }
    }
    |  IDENT_ALL compare_type IDENT_ALL
    {
	$$= &types.ComparisonExprLSRS{Left: $1,Operator:$2, Right:$3 }
    }
    | expr_opt AND expr_opt
    {
        left:=$1
        right:=$3
    	$$=&types.AndExpr{Left:left,Right:right,LeftNum:left.GetTargetColsNum(),RightNum:right.GetTargetColsNum(),}
    }
    | expr_opt OR expr_opt
    {
        left:=$1
        right:=$3
    	$$=&types.OrExpr{Left:left,Right:right,LeftNum:left.GetTargetColsNum(),RightNum:right.GetTargetColsNum(),}
    }
    | NOT expr_opt
    {
        left:=$2
    	$$=&types.NotExpr{Expr:left,LeftNum:left.GetTargetColsNum(),}
    }

value_list:
  value
  {
    $$ = make([]value.Value, 0, 1)
    $$ = append($$, $1)
  }
  | value_list ',' value
  {
    $$ = append($1, $3)
  }

value:
    {
    $$=value.Bytes{}
    }
    | string_value
    {
    $$=value.Bytes{Val:[]byte($1)}
    }
    | int64_value
    {
    $$=value.Int{Val:$1}
    }
    | float64_value
    {
    $$=value.Float{Val:$1}
    }
    | TRUE
    {
    $$=value.Bool{Val:true}
    }
    | FALSE
    {
    $$=value.Bool{Val:false}
    }
    | NULL
    {
    $$=value.Null{}
    }
compare_type:
 '=' {$$= value.Equal}
 | '<' {$$ = value.Less}
 | '>' {$$ = value.Great}
 | LE { $$ = value.LessEqual}
 | GE { $$ = value.GreatEqual}
 | NE { $$ = value.NotEqual}


limit_opt:
   {
   $$ =types.Limit{}
   }
   | LIMIT int_value
   {
   	$$=types.Limit{Rowcount:$2}
   }
   | LIMIT int_value ',' int_value
   {
      $$=types.Limit{Offset:$2, Rowcount:$4}
   }
   | LIMIT int_value OFFSET int_value
   {
      $$=types.Limit{Offset:$2, Rowcount:$4}
   }
int_value:
  decimal_value
  {
    v, _ :=strconv.Atoi($1)
    $$ = v
  }
  | hex_value
  {
    v, _ := strconv.ParseInt($1, 16, 32)
    $$ = int(v)
  }
int64_value:
    decimal_value
  {
    v, _ := strconv.ParseInt($1, 10, 64)
    $$ = v
  }
  | hex_value
  {
    v, _ := strconv.ParseInt($1, 16, 64)
    $$ = v
  }

float64_value:
    float_value
    {
      v, _ := strconv.ParseFloat($1, 0)
      $$ = v
    }

IDENT_ALL:
    IDENT
    {
      $$=$1
    }
    | IDENT_LEGAL
    {
      $$=$1
    }