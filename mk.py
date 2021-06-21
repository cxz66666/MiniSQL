name = ['alice', 'bob', 'charlie', 'dave', 'eve', 'issac', 'ivan', 'justin', 'mallory', 'matilda', 'oscar', 'pat']
import random
test_num = 100000
with open("{}.txt".format(test_num), "w") as f:
    f.write("create database cxz;\nuse database cxz;\ncreate table table1 (\ncolumn1 int not null,\ncolumn2 float,\ncolumn3 char(30) not null,\nprimary key (column1)\n);")
    for i in range(test_num):
        s = "insert into table1 values ({}, {:.2f}, '{}');\n".format(i, i, name[random.randint(0, len(name)-1)])
        f.write(s)

