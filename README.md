# MIT6.824-spring21
main: my implementation of golang tutorial "A Tour of Go"'s final exercise

使用Mutex实现

其中添加所有的channel都是为了实现：子级crawer发送“执行完成”信号给父级crawer，从而实现父级crawer对子级crawler的等待。

经过lecture中的指点，这个功能实际上可以用sync.WaitGroup实现。
