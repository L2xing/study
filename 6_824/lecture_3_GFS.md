# GFS
## 1. 目标
* storage (存储)
* _consistency （一致性）_ 


## 2. Storage
主要的目标：_Building FT Storage (构建可容错的存储能力)_。因为app一般是是stateless无状态，以来storage 持久所有数据。

挑战点：
* 高性能 （high performance）
为了提高吞吐，意味着你需要协调多台机器，进行数据分片存储。
比如有2TB的数据如果分布存储，就可以更快或多更多磁盘和网卡的性能支持。

* 太多的机器意味异常处理是常态的。 
通过引入容错解决

* 容错的解决方案就是复制

* 复制则会导致机器中的不一致行为
可以通过设计一致性方案解决

* 一致性方案通常会导致性能下降。

> 梳理一下：为了高性能引入多机器，多机器意味着异常概率的方法引入容错方案，容错的复制逻辑则会引出一致性问题，为了解决一致性问题就需要设计一致性方案，然而一致性方案也会导致性能的降低。
最终发现为了高性能链路会导致性能反而被影响。所以权衡利弊是分布式系统的程序员的最重要的议题。

******
理想的一致性：行为如同单机服务。
挑战：并发，异常。

如何理解一致性方案的必要性，一个简单的例子：
存在两个服务，server-1 和 server-2
两个写-client, w-client-1 和 w-client-2
两个读-client, r-client-1 和 r-client-2
假设一致性的方案就是单出的双写，w-client-1进行put(x=1), w-client-2进行put(x=2)，分别对server-1和server-2写入。那么r-client-1和r-client-2的读取的值将无法预测。

****
## 3. GFS讲述
GFS论文的理论在当时是被认为容易理解，但是最终理论的落地和1000台以上的机器实践却很少。因为这里面有些非标准的设计，缺乏规范化的描述，阻碍其落地困难。
* 单master节点。无容错。
构建分布式文件系统，却存在单点问题.
>
> 论文原话
> 如果master故障，则终止MapReduce计算。client可以检测到该状态，如果有需要可以重试MapReduce操作。
>
> 
>therefore our current implementation aborts the MapReduce computation
> if the master fails. Clients can check for this condition
> and retry the MapReduce operation if they desire.
> 

* 存在不一致行为。
没有做到完全的强一致性 

（高性能-高吞吐 ）
mapreduce中描述GFS在2000s时的读取速度在10000MB/s,而当时的单硬盘的读取速度大概在30MB/ 

GFS令老师惊讶的几个特点：
1. 数据规模之大。 
2. 读写速度快。 （数据分片，客户端并行）
3. 对外提供全局一致性。 （对所有客户端的行为一致 ）
4. 容错能力。 






