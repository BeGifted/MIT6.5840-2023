# 前置知识
线性一致（强一致）：对于整个请求历史记录，只存在一个序列，不允许不同的客户端看见不同的序列；或者说对于多个请求可以构造一个带环的图，那就证明不是线性一致的请求记录。
线性一致不是有关系统设计的定义，这是有关系统行为的定义。
Zookeeper 的确允许客户端将读请求发送给任意副本，并由副本根据自己的状态来响应读请求。副本的 Log 可能并没有拥有最新的条目，所以尽管系统中可能有一些更新的数据，这个副本可能还是**会返回旧的数据**。
Zookeeper 的一致性保证：写请求是线性一致的、 FIFO 客户端序列（所有客户端发送的请求以一个特定的序列执行，通过 zxid 维护）。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/39241706/1702026873677-6d7cf111-6e4e-40f8-822a-672b346e08d1.png#averageHue=%23fbfbfa&clientId=u670db1ab-564f-4&from=paste&height=831&id=u9d730766&originHeight=831&originWidth=1207&originalType=binary&ratio=1&rotation=0&showTitle=false&size=166921&status=done&style=none&taskId=u33955581-05c8-480d-b9fe-d1f59ac9f63&title=&width=1207)
# 实验内容
使用 lab2 中的 Raft 库构建 Fault-tolerant K/V Service，即维护一个简单的键/值对数据库，其中键和值都是字符串。具体来说，该服务是一个复制状态机，由多个使用 Raft 进行复制的键/值服务器组成，只要大多数服务器处于活动状态并且可以通信，该服务就应该继续处理客户端请求。
# 实验环境
OS：WSL-Ubuntu-18.04
golang：go1.17.6 linux/amd64
# Part A: Key/value service without snapshots
每一个 kvserver 都带有一个 raft，client 将 Put()、Append()、Get() 等 rpc 发送给带有 leader raft 的 kvserver。
kvserver leader：

1. 接收 client 发送的 rpc request；
2. kvserver 将这些 rpc 封装成 Op，并通过 Start() 发送给 raft，同时在 waitCh 上定时等待执行结果；
3. 过半 raft 日志复制成功，raft leader 向 applyCh 发送 ApplyMsg；
4. 所有 kvserver 收到 ApplyMsg 都会执行相应的命令，其中 kvserver leader 将执行结果通知 waitCh；
5. 超时或者 waitCh 中有结果，发送 rpc response；

其他注意的点：

- 重复或过期的 Put、Append 命令，通过 ClientId 和 ReqId 判断；
- 向 waitCh 信道内发送消息前检查是否存在，避免因为超时信道被释放；
- applyCh 的性能问题，测试要求每 100ms 完成 3 个 req，要么将 raft 中的 apply() 定时器设置成 30ms 内，要么修改代码将 applyCh 的更新改成条件变量。
> 只要是达成共识的命令必须得“执行”，否则维护的键值数据库中的数据会出问题，不满足线性一致性。“执行” 表明的是走到执行那一步，若发现是过期的 RPC 或已经执行过的命令当然不会对 Put 或 Append 执行两次。
> 其它如当前 KVServer 底层的 Raft 不再是 Leader 或者 底层的 Raft 又重新当选了 Leader (但是任期发生了改变) 等异常情况影响的是当前 KVServer 是否需要给 waitCh 发通知而不是是否执行命令，发通知表明希望 KVServer 给 Client 响应。需要满足线性一致性 (可能客户端发起 Get 时应该获取的结果是 0, 但是在次期间增加了 1。若现在回复的话会回复 1, 但是根据请求时间来看应该返回 0)。

# Part B: Key/value service with snapshots
修改 KVServer 与 raft 合作，以节省日志空间，并使用 Lab 2d 的 raft 的 Snapshot() 方法减少重新启动时间。
测试时会给 StartKVServer() 传递 maxraftstate 参数，它表明持久化的 Raft 状态的最大以字节数（包括日志，但不包括快照）；因此若调用 persister.RaftStateSize() 后发现当前持久化状态大小接近阈值 maxraftstate 的话，就应该调用 Raft 的 Snapshot() 方法来保存快照；若 maxraftstate 为 -1，表明不必创建快照。
所有 kvserver 收到 ApplyMsg 都会执行相应的命令，其中 kvserver leader 将执行结果通知 waitCh；kvserver follower 会收到由 leader 发来的快照更新自身状态。
除此以外，所有 kvserver 收到 ApplyMsg 执行相应的命令后（命令被过半数 raft 提交），应该检查自身持久化状态大小，并根据上述条件及时保存快照。
快照内容：kvs 和 lastReq



















