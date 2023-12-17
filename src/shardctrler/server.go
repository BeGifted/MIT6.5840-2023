package shardctrler

import (
	"log"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead    int32
	waitCh  map[int]chan *Op
	lastReq map[int64]int64
	timeout time.Duration

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	ClientId int64
	ReqId    int64
	OpType   string
	Servers  map[int][]string
	GIDs     []int // Leave
	Shard    int   // Move
	GID      int   // Join\Move
	Num      int   // Query
	Config   Config
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.isInvalidReq(args.ClientId, args.ReqId) {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
		OpType:   "Join",
		Servers:  args.Servers,
	}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	waitCh, ok := sc.waitCh[index]
	if !ok {
		sc.waitCh[index] = make(chan *Op, 1)
		waitCh = sc.waitCh[index]
	}
	sc.mu.Unlock()

	select {
	case <-waitCh:
		nowTerm, isLeader := sc.rf.GetState()
		if isLeader && nowTerm == term {
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(sc.timeout):
		reply.WrongLeader = true
	}

	sc.mu.Lock()
	delete(sc.waitCh, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.isInvalidReq(args.ClientId, args.ReqId) {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
		OpType:   "Leave",
		GIDs:     args.GIDs,
	}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	waitCh, ok := sc.waitCh[index]
	if !ok {
		sc.waitCh[index] = make(chan *Op, 1)
		waitCh = sc.waitCh[index]
	}
	sc.mu.Unlock()

	select {
	case <-waitCh:
		nowTerm, isLeader := sc.rf.GetState()
		if isLeader && nowTerm == term {
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(sc.timeout):
		reply.WrongLeader = true
	}

	sc.mu.Lock()
	delete(sc.waitCh, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.isInvalidReq(args.ClientId, args.ReqId) {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
		OpType:   "Move",
		Shard:    args.Shard,
		GID:      args.GID,
	}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	waitCh, ok := sc.waitCh[index]
	if !ok {
		sc.waitCh[index] = make(chan *Op, 1)
		waitCh = sc.waitCh[index]
	}
	sc.mu.Unlock()

	select {
	case <-waitCh:
		nowTerm, isLeader := sc.rf.GetState()
		if isLeader && nowTerm == term {
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(sc.timeout):
		reply.WrongLeader = true
	}

	sc.mu.Lock()
	delete(sc.waitCh, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.isInvalidReq(args.ClientId, args.ReqId) {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
		OpType:   "Query",
		Num:      args.Num,
	}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	waitCh, ok := sc.waitCh[index]
	if !ok {
		sc.waitCh[index] = make(chan *Op, 1)
		waitCh = sc.waitCh[index]
	}
	sc.mu.Unlock()

	select {
	case res := <-waitCh:
		nowTerm, isLeader := sc.rf.GetState()
		if isLeader && nowTerm == term {
			reply.Err = OK
			reply.Config = res.Config
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(sc.timeout):
		reply.WrongLeader = true
	}

	sc.mu.Lock()
	delete(sc.waitCh, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) isInvalidReq(clientId int64, reqId int64) bool {
	if lastReqId, ok := sc.lastReq[clientId]; ok {
		if reqId <= lastReqId {
			return true
		}
	}
	return false
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) exec(op *Op) {
	if sc.isInvalidReq(op.ClientId, op.ReqId) {
		return
	}
	switch op.OpType {
	case "Join":
		sc.execJoin(op)
	case "Leave":
		sc.execLeave(op)
	case "Move":
		sc.execMove(op)
	case "Query":
		sc.execQuery(op)
	}
	// update last req id
	if lastReqId, ok := sc.lastReq[op.ClientId]; ok {
		if op.ReqId > lastReqId {
			sc.lastReq[op.ClientId] = op.ReqId
		}
	} else {
		sc.lastReq[op.ClientId] = op.ReqId
	}
}

func (sc *ShardCtrler) execJoin(op *Op) {
	n := len(sc.configs)                // config num
	newGroups := sc.copyGroups()        // gid -> servers[]
	newShards := sc.configs[n-1].Shards // shard -> gid
	for gid, servers := range op.Servers {
		newGroups[gid] = servers
	}
	sc.balance(newGroups, &newShards)
	sc.configs = append(sc.configs, Config{
		Num:    sc.configs[n-1].Num + 1,
		Shards: newShards,
		Groups: newGroups,
	})
}

func (sc *ShardCtrler) execLeave(op *Op) {
	n := len(sc.configs)                // config num
	newGroups := sc.copyGroups()        // gid -> servers[]
	newShards := sc.configs[n-1].Shards // shard -> gid
	for _, gid := range op.GIDs {
		for shardId, tmpgid := range newShards {
			if gid == tmpgid {
				newShards[shardId] = 0 // wait for allocation
			}
		}
		delete(newGroups, gid)
	}
	sc.balance(newGroups, &newShards)
	sc.configs = append(sc.configs, Config{
		Num:    sc.configs[n-1].Num + 1,
		Shards: newShards,
		Groups: newGroups,
	})
}

func (sc *ShardCtrler) execMove(op *Op) {
	n := len(sc.configs)                // config num
	newGroups := sc.copyGroups()        // gid -> servers[]
	newShards := sc.configs[n-1].Shards // shard -> gid
	newShards[op.Shard] = op.GID
	sc.configs = append(sc.configs, Config{
		Num:    sc.configs[n-1].Num + 1,
		Shards: newShards,
		Groups: newGroups,
	})
}

func (sc *ShardCtrler) execQuery(op *Op) {
	n := len(sc.configs) // config num
	num := op.Num
	if num == -1 || num >= n {
		op.Config = sc.configs[n-1]
	} else {
		op.Config = sc.configs[num]
	}
}

func (sc *ShardCtrler) copyGroups() map[int][]string {
	n := len(sc.configs)
	newGroups := make(map[int][]string, len(sc.configs[n-1].Groups))
	for gid, servers := range sc.configs[n-1].Groups {
		copyServers := make([]string, len(servers))
		copy(copyServers, servers)
		newGroups[gid] = copyServers
	}
	return newGroups
}

func (sc *ShardCtrler) balance(newGroups map[int][]string, newShards *[NShards]int) {
	if len(newGroups) == 0 {
		return
	}

	cnt := map[int][]int{} // shard ids in each gid
	for gid := range newGroups {
		cnt[gid] = make([]int, 0)
	}
	for shardId, gid := range newShards {
		cnt[gid] = append(cnt[gid], shardId)
	}
	for {
		maxGID, maxNum, minGID, minNum := sc.findMinMaxGID(cnt)
		if maxGID != 0 && maxNum <= minNum+1 {
			return
		}
		cnt[minGID] = append(cnt[minGID], cnt[maxGID][0])
		cnt[maxGID] = cnt[maxGID][1:]
		newShards[cnt[minGID][len(cnt[minGID])-1]] = minGID
	}
}

func (sc *ShardCtrler) findMinMaxGID(cnt map[int][]int) (int, int, int, int) {
	minGID, maxGID := -1, -1
	minNum, maxNum := math.MaxInt, math.MinInt
	gids := []int{}
	for gid := range cnt {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	for _, gid := range gids {
		n := len(cnt[gid]) // shard num
		if maxGID != 0 &&
			((gid == 0 && n > 0) || (gid != 0 && n > maxNum)) {
			maxGID = gid
			maxNum = n
		}
		if gid != 0 && n < minNum {
			minGID = gid
			minNum = n
		}
	}
	return maxGID, maxNum, minGID, minNum
}

func (sc *ShardCtrler) apply() {
	for sc.killed() == false {
		applyMsg := <-sc.applyCh
		sc.mu.Lock()
		if op, ok := applyMsg.Command.(Op); ok {
			log.Println("exec command")
			sc.exec(&op)
			term, isLeader := sc.rf.GetState()
			if isLeader && applyMsg.CommandTerm == term {
				if waitCh, ok := sc.waitCh[applyMsg.CommandIndex]; ok {
					waitCh <- &op
				}
			}
		}
		sc.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.timeout = 500 * time.Millisecond
	sc.waitCh = make(map[int]chan *Op)
	sc.lastReq = make(map[int64]int64)

	go sc.apply()

	return sc
}
