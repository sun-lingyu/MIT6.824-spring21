# MIT6.824-spring21

LAB 3A finish

受lab2启发，利用专门的applylistener线程监听applyCh.

一个可能需要改进的点是：Op的类型字段用字符串来表示了。可能改成枚举类型更合适。

受 https://zhuanlan.zhihu.com/p/130671334 启发，修改了raft_leader。使得在新leader选举成功时向applyCh发送一条消息通知上层service。这样可以避免新term中，由于无新请求，导致pending request不能终止。
