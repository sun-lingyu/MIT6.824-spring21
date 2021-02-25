# MIT6.824-spring21
MapReduce implementation.

因为安装不上go1.15，使用了go1.13版本
所有测试通过

但是时间所限，没有实现选做的Backup Tasks (Section 3.6)
coordinator中的数据结构(maplist和reducelist)均采用了slice实现。可能采用map实现会更快，但是更复杂。
