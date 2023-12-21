package shardkv

import (
	"bytes"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/shardctrler"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	ReqId    int64
	OpType   string
	Key      string
	Value    string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead              int32
	manager           *shardctrler.Clerk
	config            shardctrler.Config
	lastConfig        shardctrler.Config
	shards            map[int]*Shard
	waitCh            map[int]chan *CommonReply
	lastReq           map[int64]int64
	persister         *raft.Persister
	lastIncludedIndex int
	timeout           time.Duration
}

const (
	Serving   = "Serving"
	Pulling   = "Pulling"
	BePulling = "BePulling"
	GCing     = "GCing" // garbage collection
)

type ShardState string

type Shard struct {
	ShardKVs map[string]string
	State    ShardState
}

func (sh *Shard) get(key string) string {
	return sh.ShardKVs[key]
}

func (sh *Shard) put(key string, value string) {
	sh.ShardKVs[key] = value
}

func (sh *Shard) append(key string, value string) {
	sh.ShardKVs[key] += value
}

func (sh *Shard) copyShard() Shard {
	newShardKVs := make(map[string]string, len(sh.ShardKVs))
	for k, v := range sh.ShardKVs {
		newShardKVs[k] = v
	}
	return Shard{
		ShardKVs: newShardKVs,
		State:    Serving,
	}
}

func (kv *ShardKV) Get(args *GetPutAppendArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	shardId := key2shard(args.Key)
	if !kv.check(shardId) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Command{
		CommandType: Get,
		Data:        *args,
	}
	response := CommonReply{}
	kv.startCommand(command, &response)
	reply.Value = response.Value
	reply.Err = response.Err
}

func (kv *ShardKV) PutAppend(args *GetPutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	shardId := key2shard(args.Key)
	if !kv.check(shardId) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.isInvalidReq(args.ClientId, args.ReqId) {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Command{
		CommandType: args.OpType,
		Data:        *args,
	}
	response := CommonReply{}
	kv.startCommand(command, &response)
	reply.Err = response.Err
}

// own this shard and state serving or gcing
func (kv *ShardKV) check(shardId int) bool {
	if shard, ok := kv.shards[shardId]; ok {
		if kv.config.Shards[shardId] == kv.gid &&
			(shard.State == Serving || shard.State == GCing) {
			return true
		}
	}
	return false
}

func (kv *ShardKV) isInvalidReq(clientId int64, reqId int64) bool {
	if lastReqId, ok := kv.lastReq[clientId]; ok {
		if reqId <= lastReqId {
			return true
		}
	}
	return false
}

func (kv *ShardKV) startCommand(command Command, response *CommonReply) {
	response.Err = OK
	index, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	waitCh, ok := kv.waitCh[index]
	if !ok {
		kv.waitCh[index] = make(chan *CommonReply, 1)
		waitCh = kv.waitCh[index]
	}
	kv.mu.Unlock()

	select {
	case res := <-waitCh:
		response.Value = res.Value
		nowTerm, isLeader := kv.rf.GetState()
		if isLeader && nowTerm == term {
			response.Err = res.Err
		} else {
			response.Err = ErrWrongLeader
		}
	case <-time.After(kv.timeout):
		response.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.waitCh, index)
	kv.mu.Unlock()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	shards := map[int]*Shard{}
	lastReq := map[int64]int64{}
	lastConfig := shardctrler.Config{}
	config := shardctrler.Config{}
	if d.Decode(&shards) != nil ||
		d.Decode(&lastReq) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&config) != nil {
		return
	} else {
		kv.shards = shards
		kv.lastReq = lastReq
		kv.lastConfig = lastConfig
		kv.config = config
	}
}

func (kv *ShardKV) apply() {
	for kv.killed() == false {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		if applyMsg.CommandValid {
			if applyMsg.CommandIndex <= kv.lastIncludedIndex {
				kv.mu.Unlock()
				continue
			}
			command, _ := applyMsg.Command.(Command)
			reply := kv.exec(command)
			nowTerm, isLeader := kv.rf.GetState()
			if isLeader && nowTerm == applyMsg.CommandTerm {
				if waitCh, ok := kv.waitCh[applyMsg.CommandIndex]; ok {
					waitCh <- reply
				}
			}
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.shards)
				e.Encode(kv.lastReq)
				e.Encode(kv.config)
				e.Encode(kv.lastConfig)
				kvstate := w.Bytes()
				kv.rf.Snapshot(applyMsg.CommandIndex, kvstate)
			}
			kv.lastIncludedIndex = applyMsg.CommandIndex
		} else if applyMsg.SnapshotValid {
			if applyMsg.SnapshotIndex <= kv.lastIncludedIndex {
				kv.mu.Unlock()
				continue
			}

			kv.readPersist(applyMsg.Snapshot)
			kv.lastIncludedIndex = applyMsg.SnapshotIndex
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) exec(command Command) *CommonReply {
	reply := CommonReply{}
	switch command.CommandType {
	case Get, Put, Append:
		op := command.Data.(GetPutAppendArgs)
		kv.execGetPutAppend(&op, &reply)
	case AddConfig:
		config := command.Data.(shardctrler.Config)
		kv.execAddConfig(&config, &reply)
	case InsertShard:
		response := command.Data.(PullShardReply)
		kv.execInsertShard(&response, &reply)
	case DeleteShard:
		args := command.Data.(RemoveShardArgs)
		kv.execDeleteShard(&args, &reply)
	case AdjustState:
		args := command.Data.(AdjustShardArgs)
		kv.execAdjustState(&args, &reply)
	}
	return &reply
}

func (kv *ShardKV) execGetPutAppend(op *GetPutAppendArgs, reply *CommonReply) {
	shardId := key2shard(op.Key)
	if !kv.check(shardId) {
		reply.Err = ErrWrongGroup
		return
	}
	reply.Err = OK
	if op.OpType == Get || !kv.isInvalidReq(op.ClientId, op.ReqId) {
		switch op.OpType {
		case Get:
			reply.Value = kv.shards[shardId].get(op.Key)
		case Put:
			kv.shards[shardId].put(op.Key, op.Value)
		case Append:
			kv.shards[shardId].append(op.Key, op.Value)
		}
		// update last req id
		if lastReqId, ok := kv.lastReq[op.ClientId]; ok {
			if op.ReqId > lastReqId {
				kv.lastReq[op.ClientId] = op.ReqId
			}
		} else {
			kv.lastReq[op.ClientId] = op.ReqId
		}
	}
}

func (kv *ShardKV) execAddConfig(config *shardctrler.Config, reply *CommonReply) {
	if config.Num == kv.config.Num+1 {
		reply.Err = OK

		for i := 0; i < shardctrler.NShards; i++ {
			// add into now
			if config.Shards[i] == kv.gid && kv.config.Shards[i] != kv.gid {
				if kv.config.Shards[i] != 0 {
					kv.shards[i].State = Pulling
				}
			}
			// add to other
			if config.Shards[i] != kv.gid && kv.config.Shards[i] == kv.gid {
				if config.Shards[i] != 0 {
					kv.shards[i].State = BePulling
				}
			}
		}

		kv.lastConfig = kv.config
		kv.config = *config
	} else {
		reply.Err = ErrOther
	}
}

func (kv *ShardKV) execInsertShard(response *PullShardReply, reply *CommonReply) {
	if response.ConfigNum == kv.config.Num {
		for shardId, shard := range response.Shards {
			if kv.shards[shardId].State == Pulling {
				for k, v := range shard.ShardKVs {
					kv.shards[shardId].ShardKVs[k] = v
				}
				kv.shards[shardId].State = GCing
			}
		}
		for clientId, reqId := range response.LastReq {
			kv.lastReq[clientId] = int64(math.Max(float64(reqId), float64(kv.lastReq[clientId])))
		}
		reply.Err = OK
	} else {
		reply.Err = ErrOther
	}
}

func (kv *ShardKV) execDeleteShard(args *RemoveShardArgs, reply *CommonReply) {
	if args.ConfigNum == kv.config.Num {
		for _, shardId := range args.ShardIds {
			if _, ok := kv.shards[shardId]; ok {
				if kv.shards[shardId].State == BePulling {
					kv.shards[shardId] = &Shard{ // delete kvs
						ShardKVs: make(map[string]string),
						State:    Serving,
					}
				}
			}

		}
		reply.Err = OK
	} else if args.ConfigNum < kv.config.Num {
		reply.Err = OK
	} else {
		reply.Err = ErrOther
	}
}

func (kv *ShardKV) execAdjustState(args *AdjustShardArgs, reply *CommonReply) {
	if args.ConfigNum == kv.config.Num {
		for _, shardId := range args.ShardIds {
			if _, ok := kv.shards[shardId]; ok {
				kv.shards[shardId].State = Serving // gcing->serving
			}
		}
		reply.Err = OK
	} else if args.ConfigNum < kv.config.Num {
		reply.Err = OK
	} else {
		reply.Err = ErrOther
	}
}

func (kv *ShardKV) addConfig() {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		isAllServing := true
		for _, shard := range kv.shards {
			if shard.State != Serving {
				isAllServing = false
				break
			}
		}
		kv.mu.Unlock()
		if isAllServing {
			config := kv.manager.Query(kv.config.Num + 1) // next config
			if config.Num == kv.config.Num+1 {
				command := Command{
					CommandType: AddConfig,
					Data:        config,
				}
				reply := CommonReply{}
				kv.startCommand(command, &reply)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) insertShard() {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		GID2ShardIds := kv.getLastGID2ShardIds(Pulling) // gid in last config
		wg := &sync.WaitGroup{}
		wg.Add(len(GID2ShardIds))
		for gid, shardIds := range GID2ShardIds {
			configNum, servers := kv.config.Num, kv.lastConfig.Groups[gid]
			go func(gid int, shardIds []int, configNum int, servers []string) {
				defer wg.Done()
				// get pulling shards in other group
				for _, server := range servers {
					args := PullShardArgs{
						GID:       gid,
						ShardIds:  shardIds,
						ConfigNum: configNum,
					}
					reply := PullShardReply{}
					srv := kv.make_end(server)
					ok := srv.Call("ShardKV.GetShards", &args, &reply)
					if ok && reply.Err == OK {
						reply.ConfigNum = configNum
						command := Command{
							CommandType: InsertShard,
							Data:        reply,
						}
						kv.startCommand(command, &CommonReply{})
					}
				}
			}(gid, shardIds, configNum, servers)
		}
		kv.mu.Unlock()
		wg.Wait()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) adjustBePulling() {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		GID2ShardIds := kv.getNowGID2ShardIds(BePulling)
		wg := &sync.WaitGroup{}
		wg.Add(len(GID2ShardIds))
		for gid, shardIds := range GID2ShardIds {
			configNum, servers := kv.config.Num, kv.lastConfig.Groups[gid]
			go func(gid int, shardIds []int, configNum int, servers []string) {
				defer wg.Done()
				// get bepulling shards in now group
				for _, server := range servers {
					args := CheckArgs{
						ShardIds:  shardIds,
						ConfigNum: configNum,
					}
					reply := CheckReply{}
					srv := kv.make_end(server)
					ok := srv.Call("ShardKV.CheckShards", &args, &reply)
					if ok && reply.Err == OK {
						args := AdjustShardArgs{
							ShardIds:  shardIds,
							ConfigNum: configNum,
						}
						command := Command{
							CommandType: AdjustState,
							Data:        args,
						}
						kv.startCommand(command, &CommonReply{})
					}
				}
			}(gid, shardIds, configNum, servers)
		}
		kv.mu.Unlock()
		wg.Wait()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) adjustGCing() {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		GID2ShardIds := kv.getLastGID2ShardIds(GCing)
		wg := &sync.WaitGroup{}
		wg.Add(len(GID2ShardIds))
		for gid, shardIds := range GID2ShardIds {
			configNum, servers := kv.config.Num, kv.lastConfig.Groups[gid]
			go func(gid int, shardIds []int, configNum int, servers []string) {
				defer wg.Done()
				// remove gcing shards in other group
				for _, server := range servers {
					args := RemoveShardArgs{
						ShardIds:  shardIds,
						ConfigNum: configNum,
					}
					reply := RemoveShardReply{}
					srv := kv.make_end(server)
					ok := srv.Call("ShardKV.DeleteShards", &args, &reply)
					if ok && reply.Err == OK {
						args := AdjustShardArgs{
							ShardIds:  shardIds,
							ConfigNum: configNum,
						}
						command := Command{
							CommandType: AdjustState,
							Data:        args,
						}
						kv.startCommand(command, &CommonReply{})
					}
				}
			}(gid, shardIds, configNum, servers)
		}
		kv.mu.Unlock()
		wg.Wait()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) getLastGID2ShardIds(state ShardState) map[int][]int {
	GID2ShardIds := make(map[int][]int)
	for shardId, shard := range kv.shards {
		if shard.State == state {
			gid := kv.lastConfig.Shards[shardId]
			if _, ok := GID2ShardIds[gid]; !ok {
				GID2ShardIds[gid] = make([]int, 0)
			}
			GID2ShardIds[gid] = append(GID2ShardIds[gid], shardId)
		}
	}
	return GID2ShardIds
}

func (kv *ShardKV) getNowGID2ShardIds(state ShardState) map[int][]int {
	GID2ShardIds := make(map[int][]int)
	for shardId, shard := range kv.shards {
		if shard.State == state {
			gid := kv.config.Shards[shardId]
			if _, ok := GID2ShardIds[gid]; !ok {
				GID2ShardIds[gid] = make([]int, 0)
			}
			GID2ShardIds[gid] = append(GID2ShardIds[gid], shardId)
		}
	}
	return GID2ShardIds
}

func (kv *ShardKV) GetShards(args *PullShardArgs, reply *PullShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if args.ConfigNum == kv.config.Num {
		log.Println("GetShards")
		shards := make(map[int]Shard)
		for _, shardId := range args.ShardIds {
			if shard, ok := kv.shards[shardId]; ok && shard.State == BePulling {
				shards[shardId] = shard.copyShard()
			}
		}
		reply.Err = OK
		reply.Shards = shards

		lastReq := make(map[int64]int64)
		for clientId, reqId := range kv.lastReq {
			lastReq[clientId] = reqId
		}
		reply.LastReq = lastReq
	} else {
		reply.Err = ErrWrongGroup
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) DeleteShards(args *RemoveShardArgs, reply *RemoveShardReply) {
	command := Command{
		CommandType: DeleteShard,
		Data:        *args,
	}
	response := CommonReply{}
	kv.startCommand(command, &response)
	reply.Err = response.Err
}

func (kv *ShardKV) CheckShards(args *CheckArgs, reply *CheckReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num > args.ConfigNum {
		reply.Err = OK
	} else {
		reply.Err = ErrOther
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Command{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetPutAppendArgs{})
	labgob.Register(PullShardReply{})
	labgob.Register(RemoveShardArgs{})
	labgob.Register(AdjustShardArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.timeout = 500 * time.Millisecond
	kv.manager = shardctrler.MakeClerk(kv.ctrlers)
	kv.config = shardctrler.Config{}
	kv.lastConfig = shardctrler.Config{}
	kv.shards = make(map[int]*Shard)
	kv.waitCh = make(map[int]chan *CommonReply)
	kv.lastReq = make(map[int64]int64)
	kv.persister = persister
	kv.readPersist(kv.persister.ReadSnapshot())

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	for i := 0; i < shardctrler.NShards; i++ {
		if _, ok := kv.shards[i]; !ok {
			kv.shards[i] = &Shard{
				ShardKVs: make(map[string]string),
				State:    Serving,
			}
		}
	}

	go kv.apply()
	go kv.addConfig()
	go kv.insertShard()
	go kv.adjustBePulling()
	go kv.adjustGCing()

	return kv
}
