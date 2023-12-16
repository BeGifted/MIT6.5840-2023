package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvs     map[string]string
	waitCh  map[int]chan *Op
	lastReq map[int64]int64
	timeout time.Duration

	persister         *raft.Persister
	lastIncludedIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
		OpType:   "Get",
		Key:      args.Key,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	waitCh, ok := kv.waitCh[index]
	if !ok {
		kv.waitCh[index] = make(chan *Op, 1)
		waitCh = kv.waitCh[index]
	}
	kv.mu.Unlock()

	// wait exec finished
	select {
	case op := <-waitCh:
		reply.Value = op.Value
		nowTerm, isLeader := kv.rf.GetState()
		if isLeader && nowTerm == term {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(kv.timeout):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.waitCh, index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.isInvalidReq(args.ClientId, args.ReqId) {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
		OpType:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	waitCh, ok := kv.waitCh[index]
	if !ok {
		kv.waitCh[index] = make(chan *Op, 1)
		waitCh = kv.waitCh[index]
	}
	kv.mu.Unlock()

	// wait exec finished
	select {
	case <-waitCh:
		nowTerm, isLeader := kv.rf.GetState()
		if isLeader && nowTerm == term {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(kv.timeout):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.waitCh, index)
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) apply() {
	for kv.killed() == false {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		if applyMsg.CommandValid {
			if applyMsg.CommandIndex <= kv.lastIncludedIndex {
				kv.mu.Unlock()
				continue
			}

			if op, ok := applyMsg.Command.(Op); ok {
				log.Println("exec command")
				kv.exec(&op)
				term, isLeader := kv.rf.GetState()
				if isLeader && applyMsg.CommandTerm == term {
					if waitCh, ok := kv.waitCh[applyMsg.CommandIndex]; ok {
						waitCh <- &op
					}
				}

				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.kvs)
					e.Encode(kv.lastReq)
					kvstate := w.Bytes()
					kv.rf.Snapshot(applyMsg.CommandIndex, kvstate)
				}
				kv.lastIncludedIndex = applyMsg.CommandIndex
			}
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

func (kv *KVServer) exec(op *Op) {
	if op.OpType != "Get" && kv.isInvalidReq(op.ClientId, op.ReqId) {
		return
	}
	switch op.OpType {
	case "Get":
		op.Value = kv.kvs[op.Key]
	case "Put":
		kv.kvs[op.Key] = op.Value
	case "Append":
		kv.kvs[op.Key] += op.Value
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

func (kv *KVServer) isInvalidReq(clientId int64, reqId int64) bool {
	if lastReqId, ok := kv.lastReq[clientId]; ok {
		if reqId <= lastReqId {
			return true
		}
	}
	return false
}

func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvs map[string]string
	var lastReq map[int64]int64
	if d.Decode(&kvs) != nil ||
		d.Decode(&lastReq) != nil {
		log.Println("decode fail")
	} else {
		kv.kvs = kvs
		kv.lastReq = lastReq
		log.Println("restore success")
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.waitCh = make(map[int]chan *Op)
	kv.lastReq = make(map[int64]int64)
	kv.timeout = 500 * time.Millisecond

	kv.persister = persister
	kv.readPersist(kv.persister.ReadSnapshot())

	go kv.apply()

	return kv
}
