#这是一个cooperation分支.每次提交代码到本分支,测试通过后统一merge到master上.

实现一个小型的数据库管理系统原型.

1.	先用邮箱注册Bitbucket账号，163邮箱可能不行（bitbucket https://bitbucket.org 是一个基于git的在线代码管理的应用，和github的功能差不多，只是bitbucket可以免费的建立小组的repository）。注册好之后，可以把邮箱告诉我一下，我给大家发邀请。然后就可以follow这个项目。项目的名字是RDBMS_TINY_RUC2016。
2.	两种方式管理项目：
（1）	SourceTree：是一个可视化的git应用，但是好像也只有windows和mac版本，windows版用起来卡卡的，mac可能会好点。
（2）	Git命令行：
使用命令行的步骤：
a)	Clone项目到本地 
git clone https://sunds@bitbucket.org/rdbms_tiny_ruc2016/rdbms_tiny_ruc2016.

b)	切换本地分支：
现在bitbucket上有两个分支master和cooperation，咱们现在都在cooperation上提交代码，测试没有问题了我在统一合并到master上。
git checkout cooperation

c)	提交
git add <file>命令向本地repo添加新建的文件。
git commit 向本地repo提交所有修改。
git push <destination> <source> 向远端提交变更。或者直接写git push，默认的是向远端与当前本地分支同名的repo提交，也就是说如果本地的checkout到了cooperation分支，那么提交也会到远端的cooperation分支，所以上一部的切换分支不要忘了。
d)	同步最新代码
git fetch 然后 git merge
或者直接git pull
# tinyDB
# tinyDB
