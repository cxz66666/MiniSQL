name = ['alice', 'bob', 'charlie', 'dave', 'eve', 'issac', 'ivan', 'justin', 'mallory', 'matilda', 'oscar', 'pat']
import random
test_num = 100000
with open("test{}.txt".format(test_num), "w") as f:
    begin = "create database cxz;\nuse database cxz;\n create table stu(\nsid int unique, grade float,\nsname char(30)\n,\nprimary key(sid)\n);\n"
    f.write(begin)
    for i in range(test_num):
        s = "insert into stu values({}, {:.2f} ,'{}');\n".format(i, i,name[random.randint(0, len(name)-1)])
        f.write(s)