package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"sync"

	//	"bytes"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state int // follower\candidate\leader

	currentTerm int     // last term server has seen
	votedFor    int     // candidateId that received vote
	log         []Entry // log entries

	commitIndex int // index of highest entry committed
	lastApplied int // index of highest entry applied to state machine

	nextIndex  []int
	matchIndex []int

	timeout    time.Duration
	expiryTime time.Time

	applyCh chan ApplyMsg
}

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

type Entry struct {
	Command interface{}
	Term    int
	Index   int // 2D: |...(lastIncludeIndex)|...(fake index)|
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Println("decode fail")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		rf.mu.Unlock()
		log.Println("restore success")
	}
}

// lastIncludedIndex
func (rf *Raft) getFirstIndex() int {
	return rf.log[0].Index
}

// lastIncludedTerm
func (rf *Raft) getFirstTerm() int {
	return rf.log[0].Term
}

func (rf *Raft) getTerm(index int) int {
	return rf.log[index-rf.getFirstIndex()].Term
}

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) resetTimeout() {
	timeout := time.Duration(250+rand.Intn(300)) * time.Millisecond
	rf.expiryTime = time.Now().Add(timeout)
	log.Println(rf.me, "reset timeout", "rf.state", rf.state)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIncludedIndex := rf.getFirstIndex()
	if lastIncludedIndex >= index {
		return
	}

	// index: fake index
	// index-lastIncludedIndex: real index
	rf.log = rf.log[index-lastIncludedIndex:]
	rf.log[0].Command = nil

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate term
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int // currentTerm
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int //leader term
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int // leader commitIndex
}

type AppendEntriesReply struct {
	Term          int // currentTerm
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm { // all server
		if rf.state != Follower {
			rf.resetTimeout()
		}
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.getLastTerm() || (args.LastLogTerm == rf.getLastTerm() && args.LastLogIndex >= rf.getLastIndex())) {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.resetTimeout()
		rf.persist()
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm { // all server
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.resetTimeout()
		rf.persist()
	}

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.PrevLogIndex < rf.getFirstIndex() {
		reply.Success = false
		reply.Term = 0
		rf.state = Follower
		rf.resetTimeout()
		return
	}

	if rf.getLastIndex() < args.PrevLogIndex ||
		rf.getTerm(args.PrevLogIndex) != args.PrevLogTerm {
		log.Println(rf.me, "mismatch", "len(rf.log)", rf.getLastIndex()+1, "args.PrevLogIndex", args.PrevLogIndex)
		reply.Success = false
		reply.Term = rf.currentTerm

		rf.resetTimeout()
		rf.state = Follower

		if args.PrevLogIndex > rf.getLastIndex() {
			reply.ConflictIndex = rf.getLastIndex() + 1
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = rf.getTerm(args.PrevLogIndex)
			for i := args.PrevLogIndex - 1; i >= rf.getFirstIndex(); i-- {
				if rf.getTerm(i) != reply.ConflictTerm {
					reply.ConflictIndex = i + 1
					break
				}
			}
		}
	} else {
		reply.Success = true
		reply.Term = rf.currentTerm

		rf.resetTimeout()
		rf.state = Follower

		// // delete
		// rf.log = rf.log[:args.PrevLogIndex+1]
		// // append
		// rf.log = append(rf.log, args.Entries...)

		insertIndex := args.PrevLogIndex + 1
		argsLogIndex := 0
		for {
			if insertIndex >= rf.getLastIndex()+1 ||
				argsLogIndex >= len(args.Entries) ||
				rf.getTerm(insertIndex) != args.Entries[argsLogIndex].Term {
				break
			}
			insertIndex++
			argsLogIndex++
		}
		// for _, e := range rf.log {
		// 	log.Print(e)
		// }
		// for _, e := range args.Entries {
		// 	log.Print(e)
		// }
		if argsLogIndex < len(args.Entries) {
			rf.log = append(rf.log[:insertIndex-rf.getFirstIndex()], args.Entries[argsLogIndex:]...)
			rf.persist()
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.getLastIndex())))
			log.Println(rf.me, "follower update commitIndex", "args.LeaderCommit", args.LeaderCommit, "len(rf.log)", rf.getLastIndex()+1, "rf.commitIndex", rf.commitIndex)
		}
	}

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm { // all server
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.resetTimeout()
		rf.persist()
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.resetTimeout()
	reply.Term = rf.currentTerm
	if rf.commitIndex >= args.LastIncludedIndex { // snapshot timeout
		return
	}

	if rf.getLastIndex() <= args.LastIncludedIndex {
		rf.log = make([]Entry, 1)
	} else {
		rf.log = rf.log[args.LastIncludedIndex-rf.getFirstIndex():]
	}
	rf.log[0].Term = args.LastIncludedTerm
	rf.log[0].Index = args.LastIncludedIndex
	rf.log[0].Command = nil

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, args.Data)

	rf.lastApplied, rf.commitIndex = args.LastIncludedIndex, args.LastIncludedIndex
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	log.Println(rf.me, "installSnapshot")
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	log.Println(rf.me, "Start", "rf.currentTerm", rf.currentTerm, "command", command, "len(rf.log)", rf.getLastIndex()+1)
	rf.log = append(rf.log, Entry{
		Command: command,
		Term:    term,
		Index:   rf.getLastIndex() + 1,
	})
	// rf.nextIndex[rf.me] = rf.getLastIndex() + 1
	// rf.matchIndex[rf.me] = rf.getLastIndex()

	rf.persist()
	index = rf.getLastIndex()
	rf.mu.Unlock()

	go rf.replicateLog() // replicate log to follower

	return index, term, isLeader
}

func (rf *Raft) replicateLog() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// rf.mu.Lock()
		// if rf.getLastIndex()+1 < rf.nextIndex[i] {
		// 	rf.mu.Unlock()
		// 	continue
		// }
		// rf.mu.Unlock()

		go func(i int) {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			for { // fail then retry
				rf.mu.Lock()
				// InstallSnapshot begin
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				lastIncludedIndex := rf.getFirstIndex()
				if rf.nextIndex[i] <= lastIncludedIndex {
					args := InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: lastIncludedIndex,
						LastIncludedTerm:  rf.getFirstTerm(),
						Data:              rf.persister.ReadSnapshot(),
					}
					rf.mu.Unlock()
					reply := InstallSnapshotReply{}
					if ok := rf.sendInstallSnapshot(i, &args, &reply); ok {
						rf.mu.Lock()
						log.Println("installSnapshot", rf.me, "sendInstallSnapshot to", i)
						if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
							rf.mu.Unlock()
							return
						}

						if reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.resetTimeout()
							rf.persist()
							rf.mu.Unlock()
							return
						}

						rf.matchIndex[i] = int(math.Max(float64(rf.matchIndex[i]), float64(args.LastIncludedIndex)))
						rf.nextIndex[i] = rf.matchIndex[i] + 1
						// rf.commit()
						rf.mu.Unlock()

					}
					return
				} // InstallSnapshot end

				rf.mu.Unlock()

				rf.mu.Lock()
				// var copylog []Entry
				// if rf.nextIndex[i] >= rf.getLastIndex()+1 {
				// 	copylog = rf.log[rf.getLastIndex()+1:]
				// } else {
				// 	copylog = rf.log[rf.nextIndex[i]-rf.getFirstIndex():]
				// }
				// prevLogIndex := rf.nextIndex[i] - 1
				// if prevLogIndex > rf.getLastIndex() {
				// 	prevLogIndex = rf.getLastIndex()
				// }
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				// copylog := rf.log[rf.nextIndex[i]-rf.getFirstIndex():]
				// prevLogIndex := rf.nextIndex[i] - 1
				// log.Println(i, "rf.nextIndex[i]", rf.nextIndex[i], "len", len(rf.log))
				prevLogIndex, nowLogIndex := rf.nextIndex[i]-1, rf.nextIndex[i]
				copylog := make([]Entry, rf.getLastIndex()+1-nowLogIndex)
				copy(copylog, rf.log[nowLogIndex-rf.getFirstIndex():])
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.getTerm(prevLogIndex),
					Entries:      copylog,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				rf.mu.Unlock()

				if ok := rf.sendAppendEntries(i, &args, &reply); ok {
					rf.mu.Lock()
					log.Println("replicateLog", rf.me, "send AppendEntries to", i, ": currentTerm=", rf.currentTerm, "reply.Term=", reply.Term, "reply.Success", reply.Success)

					if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
						rf.mu.Unlock()
						return
					}

					if reply.Term > rf.currentTerm { // all server
						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.resetTimeout()
						rf.persist()
						rf.mu.Unlock()
						return
					}

					if reply.Success {
						rf.matchIndex[i] = int(math.Max(float64(rf.matchIndex[i]), float64(args.PrevLogIndex+len(args.Entries))))
						rf.nextIndex[i] = rf.matchIndex[i] + 1
						rf.commit() // update commitIndex
						rf.mu.Unlock()
						return
					} else {
						// if args.PrevLogIndex > 0 {
						// 	rf.nextIndex[i] = args.PrevLogIndex
						// }
						if reply.ConflictTerm > 0 {
							lastIndex := -1
							for i := args.PrevLogIndex - 1; i >= rf.getFirstIndex(); i-- {
								if rf.getTerm(i) == reply.ConflictTerm {
									lastIndex = i
									break
								} else if rf.getTerm(i) < reply.ConflictTerm {
									break
								}
							}
							if lastIndex > 0 {
								rf.nextIndex[i] = lastIndex + 1
							} else {
								rf.nextIndex[i] = int(math.Max(float64(rf.matchIndex[i]+1), float64(reply.ConflictIndex)))
							}
						} else {
							rf.nextIndex[i] = int(math.Max(float64(rf.matchIndex[i]+1), float64(reply.ConflictIndex)))
						}
						// args.PrevLogIndex = rf.nextIndex[i] - 1
						// args.PrevLogTerm = rf.getTerm(rf.nextIndex[i] - 1)
						// args.Entries = rf.log[rf.nextIndex[i]-rf.getFirstIndex():]
					}
					rf.mu.Unlock()
				}

				time.Sleep(time.Duration(60) * time.Millisecond)
			}
		}(i)

	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) heartBeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				// InstallSnapshot begin
				lastIncludedIndex := rf.getFirstIndex()
				if rf.nextIndex[i] <= lastIncludedIndex {
					args := InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: lastIncludedIndex,
						LastIncludedTerm:  rf.getFirstTerm(),
						Data:              rf.persister.ReadSnapshot(),
					}
					rf.mu.Unlock()
					reply := InstallSnapshotReply{}
					if ok := rf.sendInstallSnapshot(i, &args, &reply); ok {
						rf.mu.Lock()

						if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
							rf.mu.Unlock()
							return
						}

						if reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.resetTimeout()
							rf.persist()
							rf.mu.Unlock()
							return
						}

						rf.matchIndex[i] = int(math.Max(float64(rf.matchIndex[i]), float64(args.LastIncludedIndex)))
						rf.nextIndex[i] = rf.matchIndex[i] + 1
						// rf.commit()
						rf.mu.Unlock()

					}
					return
				} // InstallSnapshot end

				// log.Println(i, "rf.nextIndex[i]", rf.nextIndex[i], "len", len(rf.log))
				// var copylog []Entry
				// if rf.nextIndex[i] >= rf.getLastIndex()+1 {
				// 	copylog = rf.log[rf.getLastIndex()+1-rf.getFirstIndex():]
				// } else {
				// 	copylog = rf.log[rf.nextIndex[i]-rf.getFirstIndex():]
				// }
				// prevLogIndex := rf.nextIndex[i] - 1
				// if prevLogIndex > rf.getLastIndex() {
				// 	prevLogIndex = rf.getLastIndex()
				// }
				// copylog := rf.log[rf.nextIndex[i]-rf.getFirstIndex():]
				// prevLogIndex := rf.nextIndex[i] - 1
				prevLogIndex, nowLogIndex := rf.nextIndex[i]-1, rf.nextIndex[i]
				copylog := make([]Entry, rf.getLastIndex()+1-nowLogIndex)
				copy(copylog, rf.log[nowLogIndex-rf.getFirstIndex():])

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.getTerm(prevLogIndex),
					Entries:      copylog,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				rf.mu.Unlock()
				if ok := rf.sendAppendEntries(i, &args, &reply); ok {
					rf.mu.Lock()
					log.Println("heartBeat", rf.me, "send AppendEntries to", i, ": currentTerm=", rf.currentTerm, "reply.Term=", reply.Term, "reply.Success", reply.Success)

					if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
						rf.mu.Unlock()
						return
					}

					if reply.Term > rf.currentTerm { // all server
						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.resetTimeout()
						rf.persist()
						rf.mu.Unlock()
						return
					}

					if reply.Success {
						rf.matchIndex[i] = int(math.Max(float64(rf.matchIndex[i]), float64(args.PrevLogIndex+len(args.Entries))))
						rf.nextIndex[i] = rf.matchIndex[i] + 1
						rf.commit() // update commitIndex
						rf.mu.Unlock()
						return
					} else {
						if reply.ConflictTerm > 0 {
							lastIndex := -1
							for i := args.PrevLogIndex - 1; i >= rf.getFirstIndex(); i-- {
								if rf.getTerm(i) == reply.ConflictTerm {
									lastIndex = i
									break
								} else if rf.getTerm(i) < reply.ConflictTerm {
									break
								}
							}
							if lastIndex > 0 {
								rf.nextIndex[i] = lastIndex + 1
							} else {
								rf.nextIndex[i] = int(math.Max(float64(rf.matchIndex[i]+1), float64(reply.ConflictIndex)))
							}
						} else {
							rf.nextIndex[i] = int(math.Max(float64(rf.matchIndex[i]+1), float64(reply.ConflictIndex)))
						}
					}
					rf.mu.Unlock()
				}
			}(i)
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (rf *Raft) toLeader() {
	rf.mu.Lock()
	// upon election
	log.Println(rf.me, "become leader")
	rf.state = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastIndex() + 1
		rf.matchIndex[i] = 0
	}
	// rf.matchIndex[rf.me] = rf.getLastIndex()
	rf.mu.Unlock()
	// rf.commit() // update commitIndex
	go rf.heartBeat()
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		if rf.state != Leader && time.Now().After(rf.expiryTime) {
			go func() { // leader selection
				rf.mu.Lock()
				rf.state = Candidate
				rf.votedFor = rf.me
				rf.currentTerm++
				rf.resetTimeout()
				rf.persist()

				numGrantVote := 1 // self grant

				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.getLastIndex(),
					LastLogTerm:  rf.getLastTerm(),
				}
				rf.mu.Unlock()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}

					go func(i int) {
						reply := RequestVoteReply{}
						if ok := rf.sendRequestVote(i, &args, &reply); ok {
							rf.mu.Lock()

							log.Println("requestVote", rf.me, "send RequestVote to", i, ": currentTerm=", rf.currentTerm, "reply.Term=", reply.Term, "reply.VoteGranted", reply.VoteGranted)

							if rf.state != Candidate || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
								rf.mu.Unlock()
								return
							}

							if reply.Term > rf.currentTerm {
								rf.state = Follower
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								rf.resetTimeout()
								rf.persist()
							} else if reply.VoteGranted {
								numGrantVote++
								if numGrantVote > len(rf.peers)/2 {
									rf.mu.Unlock()
									rf.toLeader()
									return
								}
							}
							rf.mu.Unlock()
						}
					}(i)
				}

			}()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) apply() {
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.commitIndex > rf.lastApplied {
			lastIncludedIndex := rf.getFirstIndex()
			logEntries := rf.log[rf.lastApplied+1-lastIncludedIndex : rf.commitIndex+1-lastIncludedIndex]
			rf.lastApplied = rf.commitIndex
			rf.mu.Unlock()
			for _, entry := range logEntries {
				log.Println(rf.me, "apply", "rf.commitIndex", rf.commitIndex, "rf.lastApplied", rf.lastApplied, "command", entry.Command)
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
					CommandTerm:  entry.Term,
				}
			}
			continue
		}

		// if rf.commitIndex > rf.lastApplied {
		// 	log.Println(rf.me, "apply", "rf.commitIndex", rf.commitIndex, "rf.lastApplied", rf.lastApplied, "command", rf.log[rf.lastApplied+1].Command)
		// 	rf.lastApplied++
		// 	rf.applyCh <- ApplyMsg{
		// 		CommandValid: true,
		// 		Command:      rf.log[rf.lastApplied].Command,
		// 		CommandIndex: rf.lastApplied,
		// 	}
		// }
		rf.mu.Unlock()

		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

func (rf *Raft) commit() {
	if rf.state != Leader {
		return
	}

	for N := rf.commitIndex + 1; N <= rf.getLastIndex(); N++ {
		if rf.getTerm(N) != rf.currentTerm {
			continue
		}
		num := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= N {
				num++
			}
		}
		if num > len(rf.peers)/2 {
			rf.commitIndex = N
			log.Println(rf.me, "commit", "rf.commitIndex", rf.commitIndex, "len(rf.log)", rf.getLastIndex()+1)
			return
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// go StartHTTPDebuger()
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.log = make([]Entry, 1)
	rf.log[0].Index = 0
	rf.log[0].Term = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower

	rf.resetTimeout()

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied = rf.getFirstIndex()
	rf.commitIndex = rf.getFirstIndex()

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastIndex() + 1
		rf.matchIndex[i] = 0
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.apply()

	return rf
}

// const (
// 	pprofAddr string = ":7890"
// )

// //http://localhost:7890/debug/pprof/goroutine
// func StartHTTPDebuger() {
// 	pprofHandler := http.NewServeMux()
// 	pprofHandler.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
// 	server := &http.Server{Addr: pprofAddr, Handler: pprofHandler}
// 	go server.ListenAndServe()
// }
