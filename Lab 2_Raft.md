# 前置知识
![raft-图1.jpg](https://cdn.nlark.com/yuque/0/2023/jpeg/39241706/1701941367634-a37659bb-5bbf-49ea-a859-ce6617ab4b31.jpeg#averageHue=%23dedfa4&clientId=u29201d15-d2b9-4&from=drop&id=u63b81e43&originHeight=319&originWidth=605&originalType=binary&ratio=1&rotation=0&showTitle=false&size=57741&status=done&style=none&taskId=u76b91d05-b5d0-4125-bc96-1dc888bf26e&title=)
## 什么是一致性算法？

- 安全性保证，绝对不会返回一个错误的结果；
- 可用性，容忍集群部分节点失败；
- 不依赖时序来保证一致性；
- 一条指令可以尽可能快的在集群中大多数节点响应一轮远程过程调用时完成。小部分比较慢的节点不会影响系统整体的性能；
## Raft
总结：
![image.png](https://cdn.nlark.com/yuque/0/2023/png/39241706/1701942529641-1d2b7c0a-35f5-4fd5-881d-3d56f40bf538.png#averageHue=%23f8f4ef&clientId=u29201d15-d2b9-4&from=paste&height=1242&id=u46ebc873&originHeight=880&originWidth=702&originalType=binary&ratio=1&rotation=0&showTitle=false&size=313917&status=done&style=none&taskId=ua97feb0f-6a8f-4312-91e6-1b2a863b191&title=&width=991)
服务器状态转移：
跟随者只响应来自其他服务器的请求。如果跟随者接收不到消息，那么他就会变成候选人并发起一次选举。获得集群中大多数选票的候选人将成为领导人。在一个任期内，领导人一直都会是领导人，直到自己宕机了。
![raft-图4.jpg](https://cdn.nlark.com/yuque/0/2023/jpeg/39241706/1701943764019-7b16cebb-6fc3-498b-acd4-eb6e027d7644.jpeg#averageHue=%23eeeeee&clientId=u29201d15-d2b9-4&from=drop&id=u73c6ca13&originHeight=285&originWidth=635&originalType=binary&ratio=1&rotation=0&showTitle=false&size=47147&status=done&style=none&taskId=uec4ec645-41d9-4540-9523-0751f29d2d0&title=)
避免脑裂：奇数个服务器，在任何时候为了完成任何操作，必须凑够过半的服务器来批准相应的操作。
> 例如，当一个Raft Leader竞选成功，那么这个Leader必然凑够了过半服务器的选票，而这组过半服务器中，必然与旧Leader的过半服务器有重叠。所以，新的Leader必然知道旧Leader使用的任期号（term number），因为新Leader的过半服务器必然与旧Leader的过半服务器有重叠，而旧Leader的过半服务器中的每一个必然都知道旧Leader的任期号。类似的，任何旧Leader提交的操作，必然存在于过半的Raft服务器中，而任何新Leader的过半服务器中，必然有至少一个服务器包含了旧Leader的所有操作。这是Raft能正确运行的一个重要因素。

应用程序代码和 Raft 库：应用程序代码接收 RPC 或者其他客户端请求；不同节点的 Raft 库之间相互合作，来维护多副本之间的操作同步。
Log 是 Leader 用来对操作排序的一种手段。Log 与其他很多事物，共同构成了 Leader 对接收到的客户端操作分配顺序的机制。还有就是能够向丢失了相应操作的副本重传，也需要存储在 Leader 的 Log 中。而对于 Follower 来说，Log 是用来存放临时操作的地方。Follower 收到了这些临时的操作，但是还不确定这些操作是否被 commit 了，这些操作可能会被丢弃。对所有节点而言，Log 帮助重启的服务器恢复状态
避免分割选票：为选举定时器随机地选择超时时间。
> broadcastTime ≪ electionTimeout ≪ MTBF(mean time between failures)

RAFT 与应用程序交互：
![image.png](https://cdn.nlark.com/yuque/0/2023/png/39241706/1702026873677-6d7cf111-6e4e-40f8-822a-672b346e08d1.png#averageHue=%23fbfbfa&clientId=u670db1ab-564f-4&from=paste&height=831&id=u9d730766&originHeight=831&originWidth=1207&originalType=binary&ratio=1&rotation=0&showTitle=false&size=166921&status=done&style=none&taskId=u33955581-05c8-480d-b9fe-d1f59ac9f63&title=&width=1207)
# 实验内容
实现 RAFT，分为四个 part：leader election、log、persistence、log compaction。
# 实验环境
OS：WSL-Ubuntu-18.04
golang：go1.17.6 linux/amd64
# Part 2A: leader election
这部分主要实现选出一位领导人，如果没有失败，该领导人将继续担任领导人；如果旧领导人 fail 或往来于旧领导人的 rpc 丢失，则由新领导人接任。
检测选举超时，发起选举；
当选后定时发送心跳；

RequestVote：

- follower 一段时间未收到心跳发送 RequestVote，转为 candidate；
- candidate 一段时间未收到赞成票发送 RequestVote，维持 candidate；
- 接收 RequestVote 的 server：
   - T < currentTerm：reply false；
   - T >= currentTerm && votedFor is nil or candidateId && 日志较旧：reply true；转为 follower；
   - else：reply false；
- RequestVoteReply：看返回的 term 如果比 currentTerm 大，转为 follower；否则计算投票数。

AppendEntries：

- 心跳，不带 entries，维持 leader；
- 日志复制，在 last log index ≥ nextIndex[i] 时触发；
- 接收 AppendEntries 的server：
   - term < currentTerm || prevLogIndex 上 entry 的 term 与 prevLogTerm 不匹配：reply false；
   - 删除冲突的 entries，添加 new entries；
   - leaderCommit > commitIndex：commitIndex = min(leaderCommit, index of last new entry)；
- AppendEntries 返回：
   - 成功：更新 nextIndex[i]、matchIndex[i]；
   - 失败：减少 nextIndex[i]，retry；
# Part 2B: log
这部分主要实现添加新的 log。
> **日志匹配特性（Log Matching Property）**：
> - 如果在不同的日志中的两个条目拥有相同的索引和任期号，那么他们存储了相同的指令。
> - 如果在不同的日志中的两个条目拥有相同的索引和任期号，那么他们之前的所有日志条目也全部相同。
> 
第一个特性来自这样的一个事实，领导人最多在一个任期里在指定的一个日志索引位置创建一条日志条目，同时日志条目在日志中的位置也从来不会改变。第二个特性由附加日志 RPC 的一个简单的**一致性检查**所保证。在发送附加日志 RPC 的时候，领导人会把新的日志条目前紧挨着的条目的索引位置和任期号包含在日志内。如果跟随者在它的日志中找不到包含相同索引位置和任期号的条目，那么他就会拒绝接收新的日志条目。一致性检查就像一个归纳步骤：一开始空的日志状态肯定是满足日志匹配特性的，然后一致性检查在日志扩展的时候保护了日志匹配特性。因此，每当附加日志 RPC 返回成功时，领导人就知道跟随者的日志一定是和自己相同的了。

Start：
向 leader 添加log；
> 客户端的每一个请求都包含一条被复制状态机执行的指令。领导人把这条指令作为一条新的日志条目附加到日志中去（此时请求返回指令在日志中的 index，但不保证指令被提交；Start 函数返回了，随着时间的推移，对应于这个客户端请求的 ApplyMsg 从applyCh channel 中出现在了 key-value 层。只有在那个时候，key-value 层才会执行这个请求，并返回响应给客户端），然后**并行**地发起 AppendEntries RPCs 给其他的服务器，让他们复制这条日志条目。当这条日志条目被安全地复制，领导人会应用这条日志条目到它的状态机中然后把执行的结果返回给客户端。如果跟随者崩溃或者运行缓慢，再或者网络丢包，领导人会**不断的重复**尝试 AppendEntries RPCs （尽管已经回复了客户端）直到所有的跟随者都最终存储了所有的日志条目。

commit：
更新 commitIndex。
> 领导人知道一条当前任期内的日志记录是可以被提交的，只要它被存储到了**大多数**的服务器上。Raft 永远不会通过计算副本数目的方式去提交一个之前任期内的日志条目。只有领导人**当前任期**里的日志条目通过计算副本数目可以被提交；一旦当前任期的日志条目以这种方式被提交，那么由于日志匹配特性，之前的日志条目也都会被间接的提交。

apply：
当 commitIndex 落后 lastApplied，向 applyCh 传输 command。
# Part 2C: persistence
![image.png](https://cdn.nlark.com/yuque/0/2023/png/39241706/1702453614212-efe2c8a2-e505-4a90-adf7-95c8935b634c.png#averageHue=%23eee9e5&clientId=udf51f584-800b-4&from=paste&height=733&id=u73292f66&originHeight=733&originWidth=626&originalType=binary&ratio=1&rotation=0&showTitle=false&size=190659&status=done&style=none&taskId=u3c482412-735c-4c35-8498-226bd2d4480&title=&width=626)
实现持久化，重启后能快速恢复。真正的实现将在每次更改时在磁盘写下 raft 的持久状态，并在重新启动后从磁盘中读取状态。lab 实现时在 Persister 中存储和恢复。currentTerm、votedFor、log[]
persist 和 readPersist：通过 labgob实现。

优化：避免 leader 每次只往前移动一位；若日志很长的话在一段时间内无法达到冲突位置。若 leader 发送心跳时接收到的回复是 false，leader 会减小发送的 AppendEntriesArgs 中的 rf.nextIndex[peerId]，但是这种每次仅减小 1 的方案无法在有限的时间内确定跟其他服务器发生冲突的下标位置（下标大概只能从600减小到400, 然后 fail）
# Part 2D: log compaction
![image.png](https://cdn.nlark.com/yuque/0/2023/png/39241706/1702453985884-fc891459-a659-41f0-9d76-d99cc16a1b71.png#averageHue=%23f8f6f3&clientId=udf51f584-800b-4&from=paste&height=823&id=u85f109ac&originHeight=823&originWidth=618&originalType=binary&ratio=1&rotation=0&showTitle=false&size=172307&status=done&style=none&taskId=u8efc3f29-9eca-4580-8cfa-e396c757c3b&title=&width=618)
> 在单个 InstallSnapshot RPC 中发送整个快照, 不要实现图中的偏移机制来分割快照。

![image.png](https://cdn.nlark.com/yuque/0/2023/png/39241706/1702570406684-62f6909b-675a-4134-8e08-fda9e840aeec.png#averageHue=%23f0eeec&clientId=ue526796e-4074-4&from=paste&height=499&id=u981a2946&originHeight=499&originWidth=594&originalType=binary&ratio=1&rotation=0&showTitle=false&size=95926&status=done&style=none&taskId=u7f50acee-a29d-4fda-a9da-73d2cf1e607&title=&width=594)
重新启动的服务器会遍历完整的 Raft 日志以恢复其状态。然而，对于长期运行的服务来说，永远记住完整的Raft日志是不现实的。这部分 lab 修改 Raft 以实现快照，此时 Raft 将丢弃快照之前的日志条目。
充分利用持久化的 log 日志的第0个条目，用来保存 LastIncludedIndex\LastIncludedTerm 。
需要更改之前所有用到log的地方，因为使用了快照保存前部分的日志，日志索引都通过条目新增的 Index 属性保存。
跑一次 150 多秒，比很多其他博客的方案快 100 多秒，原因将心跳发送和日志复制分开来处理的结果，心跳是 leader 定时发送的，日志复制则是当 service 向 leader 添加日志后，leader 单独执行的 goroutine。


























