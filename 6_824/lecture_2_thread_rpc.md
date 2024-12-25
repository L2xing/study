# Lecture2- Thread & RPC (golang)
golang: 一半的OOP，高性能，简洁干练而又强大，现代化（模块管理， 携程并发， lock处理）, 类型安全；一板一眼，拓展性存疑。

Why Go 去实现分布式？
1. 对携程和RPC支持更好，lock处理
2. GC (memory safe)
3. type safe
4. simple
5. complier 编译时开销小
6. 代码提示（编译时提示）

******
线程
线程在分布式的必要性原因？
1. I/O  并发
2. 多核并发
3. 方便处理一些异步逻辑

*** 
使用线程的挑战
1. 并发竞争
n=n+1;
 解决方案：
 * 不要共享变量 （channal）
 * lock

2.  多线程协调 (线程通信)
解决方案：
* channals
* condition variables （条件变量） 

3. DeadLock (死锁)

****** 
golang 应对以上问题大体的解决方案有两种。
1. channel  (no sharing)
2. locks + condition variables 
没有谁更优，而是在不同场景下看谁更加合适。
case1: 开发一个内存的KV server, 我就倾向locks + condition variables.
case2: 开发一个异步任务阻塞队列，我就倾向于使用channel。

多线程小例子但是可以说明很多问题：
```golang
package main

import "time"
import "math/rand"

func main() {
	rand.Seed(time.Now().UnixNano())

	count := 0
	finished := 0

	for i := 0; i < 10; i++ {
		go func() {
			vote := requestVote()
			if vote {
				count++
			}
			finished++
		}()
	}

	for count < 5 && finished != 10 {
		// wait
	}
	if count >= 5 {
		println("received 5+ votes!")
	} else {
		println("lost")
	}
}

func requestVote() bool {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return rand.Int() % 2 == 0
}
```
问题1: 共享变量有哪些？
问题2: 并发问题是否存在
问题3: lock怎么用
问题4: for循环i的值在不同的携程里是什么值
问题5: condition解决for循环的loop问题 (sync.NewCond(&Mutex))
问题6:使用channel怎么做解决这个问题 
问题7:golang怎么防止携程泄漏？
.....

gc算法对象判断，是否被其他对象引用，是否直接被线程引用？

******
例子：Crawler (爬虫)
目标：
1. I/O 并发性。
2. 不重复爬
3. 多核并行和多线程并发

* 可选个人解决方案： ???
golang.org/ 
爬取方案

********
# RPC （remote procedure call）
RPC VS PC

RPC例子：
client,  执行方
PUT,     执行方法
args,    入参
reply,   出参 （可能会有个error）
client.call("PUT", &args, &reply)

******
RPC的异常case（确保完成一次c/s交互）,使得它和PC有很大的不一样。
1. at-least-once(至少一次)，可能会被多次调用。
客户端重试实现。用的比较少

2. at-most-once(最多一次) 
通过过滤重复实现（duplicate）,服务器需要支持。
RPC使用最多的。
golang的rpc默认是这个case。

3. exactly-once (正好一次)
比较难。Lab3的时候需要实现。


*******
# 总结
这个单元讲了一下golang的携程和rpc这两个在lab中需要用到的内容。
但是都是入门级别的如果已经有了golang的开发经验其实是可以不用看的。
贴近实现而非理论。

 






















 