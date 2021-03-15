# MIT6.824-spring21

Lab2B finish

相比于Lab2A，主要修改了leader部分

1. 将leader的各部分封装成函数（否则太长）

2. 将通知leader终止的通道leaderAbortChannel chan bool改为条件变量leaderAbortcond *sync.Cond。

测试50次，通过47次，错误3次。测试结果可以在testoutput.txt中找到。

我感觉，这三次错误应该都是由于electAbortChannel导致的。如果把它也替换成条件变量，问题应该可以解决。

但是工作量略大，以后有时间再改。
