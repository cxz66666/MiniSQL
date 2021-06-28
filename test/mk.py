name = ['alice', 'bob', 'charlie', 'dave', 'eve', 'issac', 'ivan', 'justin', 'mallory', 'matilda', 'oscar', 'pat']
import random

import string

def generate_random_str( randomlength=1):
        """
        生成一个指定长度的随机字符串，其中
        string.digits=0123456789
        string.ascii_letters=abcdefghigklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ
        """
        str_list = [random.choice(string.digits + string.ascii_letters) for i in range(randomlength)]
        random_str = ''.join(str_list)
        return random_str

test_name = 'test100000'
test_num=100000
with open("{}.txt".format(test_name), "w") as f:
    f.write("create database cxz;\nuse database cxz;\ncreate table table1 (\ncolumn1 int unique,\ncolumn2 float not null,\ncolumn3 char(30) ,\nprimary key (column1)\n);")
    # f.write("create index i1 on table1(column1);\n")
    # f.write("create index i2 on table1(column2);\n")

    for i in range(test_num):
        s = "insert into table1 values ({}, {:.2f}, '{}');\n".format(i, i, generate_random_str(30))
        f.write(s)
