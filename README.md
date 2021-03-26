# MIT6.824-spring21

Lab2C finish

完美实现。2A 2B 2C的testcase各运行50次，没有发现问题。

对leader和candidate都进行了较大的改动

最重要的改动是取消了用于通知leader/candidate退出的channel/cond，改为利用rf.currentTerm==currentTerm和对rf.state的判断进行退出检测。

具体的实现描述见此https://www.cnblogs.com/sun-lingyu/p/14579769.html

开心！
