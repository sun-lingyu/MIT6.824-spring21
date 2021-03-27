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

	"fmt"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"

	"math/rand"
	"time"
)

type serverState int

const (
	follower                serverState   = iota
	candidate               serverState   = iota
	leader                  serverState   = iota
	electionTimeoutStart    time.Duration = 800 * time.Millisecond //800
	electionTimeoutInterval time.Duration = 200 * time.Millisecond
	heartbeatInterval       time.Duration = 200 * time.Millisecond //200
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// The struct of a log entry
//
type LogEntry struct {
	Term    int
	Command interface{}
}

type logType struct {
	Entries           []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
}

func (l *logType) index(index int) LogEntry {
	if index > l.LastIncludedIndex+len(l.Entries) {
		panic("ERROR: index greater than log length!\n")
	} else if index < l.LastIncludedIndex {
		panic("ERROR: index smaller than log snapshot!\n")
	} else if index == l.LastIncludedIndex {
		//fmt.Printf("WARNING: index == l.LastIncludedIndex\n")
		return LogEntry{Term: l.LastIncludedTerm, Command: nil}
	}
	return l.Entries[index-l.LastIncludedIndex-1]
}
func (l *logType) lastIndex() int {
	return l.LastIncludedIndex + len(l.Entries)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).

	//Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         logType

	//Volatile state on all servers:
	commitIndex int
	lastApplied int

	//Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	//Volatile state on all servers:
	//(added by me
	state serverState //0:follower; 1:candidate; 2:leader
	//tickerResetChannel chan bool
	applyCh   chan ApplyMsg
	timer     *time.Timer
	timerLock sync.Mutex
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//added in lab2B:
	newLogCome *sync.Cond //only valid in leader state

	getVote                        int
	heartbeatTimer                 time.Timer
	heartbeatTimerTerminateChannel chan bool

	//added in lab2D
	snapshot  []byte
	applyCond *sync.Cond
}

func (rf *Raft) resetTimer() {
	rf.timerLock.Lock()
	//timer must first be stopped, then reset.
	if !rf.timer.Stop() {
		//this may go wrong, but very unlikely.
		//see here: https://zhuanlan.zhihu.com/p/133309349
		select {
		case <-rf.timer.C: //try to drain from the channel
		default:
		}
	}
	duration := time.Duration(rand.Int())%electionTimeoutInterval + electionTimeoutStart
	rf.timer.Reset(duration)
	rf.timerLock.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log.Entries = make([]LogEntry, 0) //empty. (first index is one)
		rf.log.LastIncludedIndex = 0
		rf.log.LastIncludedTerm = -1
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log logType
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Printf("server %d readPersist: decode error!", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		//fmt.Printf("CondInstallSnapshot refused\n")
		return false
	}

	defer func() {
		rf.log.LastIncludedIndex = lastIncludedIndex
		rf.log.LastIncludedTerm = lastIncludedTerm
		rf.snapshot = snapshot
		rf.commitIndex = lastIncludedIndex //IMPORTANT
		rf.lastApplied = lastIncludedIndex //IMPORTANT
		rf.persistStateAndSnapshot(snapshot)

	}()
	if lastIncludedIndex <= rf.log.lastIndex() && rf.log.index(lastIncludedIndex).Term == lastIncludedTerm {
		rf.log.Entries = append([]LogEntry(nil), rf.log.Entries[lastIncludedIndex-rf.log.LastIncludedIndex:]...)
		return true
	}

	//discard the entire log
	rf.log.Entries = make([]LogEntry, 0)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d: Snapshot create\n", rf.me)
	if index <= rf.log.LastIncludedIndex {
		//already created a snapshot
		return
	}
	rf.log.Entries = append([]LogEntry(nil), rf.log.Entries[index-rf.log.LastIncludedIndex:]...)
	rf.log.LastIncludedIndex = index
	rf.log.LastIncludedTerm = rf.log.index(index).Term
	rf.snapshot = snapshot
	rf.persistStateAndSnapshot(snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	DPrintf("server %d receive InstallSnapshot from %d\n", rf.me, args.LeaderID)
	defer rf.mu.Unlock()
	defer func() { reply.Term = rf.currentTerm }()

	switch {
	case args.Term < rf.currentTerm:
		//outdated request
		DPrintf("server %d: InstallSnapshot, args.Term%d < rf.currentTerm%d\n", rf.me, args.Term, rf.currentTerm)
		return

	case args.Term > rf.currentTerm:
		//we are outdated
		rf.currentTerm = args.Term
		rf.persist()

		if rf.state != follower {
			rf.state = follower
		}

	case args.Term == rf.currentTerm:
		//normal
		if rf.state == leader {
			fmt.Printf("ERROR! Another leader in current term?!") //impossible
		} else if rf.state == candidate {
			//fmt.Printf("Candidate %d abdicate!\n", rf.me)
			rf.state = follower

		}
	}

	if args.LastIncludedIndex <= rf.log.LastIncludedIndex {
		//coming snapshot is older than our snapshot
		DPrintf("WARNING: outdated InstallSnapshot. This should only appear in unreliable cases.\n")
		return
	}
	rf.resetTimer()

	msg := ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotIndex: args.LastIncludedIndex, SnapshotTerm: args.LastIncludedTerm}
	go func() { rf.applyCh <- msg }()

}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term                   int
	Success                bool
	ConflictEntryTerm      int
	ConflictTermFirstIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//IMPORTANT
	//RPC handler do not change state itself.
	//It only send Abort messages to leader() or elect().
	//changing state is done by them
	//otherwise leader() or candidate() may be confused about their state.
	rf.mu.Lock() //hold the lock during entire handle process
	DPrintf("server %d receive AppendEntries from %d\n", rf.me, args.LeaderID)
	defer rf.mu.Unlock()
	defer func() { reply.Term = rf.currentTerm }()

	reply.ConflictEntryTerm = -1
	reply.ConflictTermFirstIndex = -1

	switch {
	case args.Term < rf.currentTerm:
		//outdated request
		DPrintf("server %d: false1, args.Term%d < rf.currentTerm%d\n", rf.me, args.Term, rf.currentTerm)
		reply.Success = false
		return

	case args.Term > rf.currentTerm:
		//we are outdated
		rf.currentTerm = args.Term
		rf.persist()

		if rf.state != follower {
			rf.state = follower
		}

	case args.Term == rf.currentTerm:
		//normal
		if rf.state == leader {
			fmt.Printf("ERROR! Another leader in current term?!") //impossible
		} else if rf.state == candidate {
			//fmt.Printf("Candidate %d abdicate!\n", rf.me)
			rf.state = follower

		}
	}

	if args.PrevLogIndex < rf.log.LastIncludedIndex {
		//outdated AppendEntries
		//only happen due to unreliable network
		reply.Success = false
		return
	}

	rf.resetTimer() //reset timer

	//lab 2b
	if args.PrevLogIndex > 0 && (args.PrevLogIndex > rf.log.lastIndex() || rf.log.index(args.PrevLogIndex).Term != args.PrevLogTerm) {
		//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.Success = false
		DPrintf("server %d: false2\n", rf.me)
		DPrintf("args.PrevLogIndex: %d,rf.log.lastIndex(): %d\n", args.PrevLogIndex, rf.log.lastIndex())

		if args.PrevLogIndex > rf.log.lastIndex() {
			reply.ConflictTermFirstIndex = rf.log.lastIndex() + 1
			reply.ConflictEntryTerm = -1
			return
		}

		reply.ConflictEntryTerm = rf.log.index(args.PrevLogIndex).Term
		conflictTermFirstIndex := args.PrevLogIndex
		for conflictTermFirstIndex >= rf.log.LastIncludedIndex && rf.log.index(conflictTermFirstIndex).Term == reply.ConflictEntryTerm {
			conflictTermFirstIndex--
		}
		reply.ConflictTermFirstIndex = conflictTermFirstIndex + 1

		return
	}
	DPrintf("reply.Success = true on server %d\n", rf.me)
	DPrintf("len(args.Entries): %d\n", len(args.Entries))

	reply.Success = true

	var i int
	for i = 0; i < len(args.Entries); i++ {
		//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		//here can't write "for i =range(args.Entries)",
		//since it will not increment i when i==len(args.Entries)-1
		//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		//it spent me 2 hours here to debug.
		if args.PrevLogIndex+i+1 > rf.log.lastIndex() {
			break
		}
		if args.Entries[i].Term == rf.log.index(args.PrevLogIndex+i+1).Term {
			continue
		}
		//If an existing entry conflicts with a new one
		//(same index but different terms),
		//delete the existing entry and all that follow it
		rf.log.Entries = rf.log.Entries[:args.PrevLogIndex+i+1-rf.log.LastIncludedIndex-1]
		rf.persist()
		break
	}

	for j := i; j < len(args.Entries); j++ {
		rf.log.Entries = append(rf.log.Entries, args.Entries[j])
		DPrintf("server %d append: %d", rf.me, rf.log.lastIndex())
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastIndex()) //index of last new entry

		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Signal()
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	//IMPORTANT
	//RPC handler do not change state itself.
	//It only send Abort messages to leader() or elect().
	//changing state is done by them
	//otherwise leader() or candidate() may be confused about their state.

	//different from AppendEntries: reset timer only when we grant the vote.
	rf.mu.Lock() //hold the lock during the whole process
	defer rf.mu.Unlock()
	defer func() { reply.Term = rf.currentTerm }()

	grantVote := func() {
		DPrintf("server %d vote for server %d\n", rf.me, args.CandidateID)
		rf.resetTimer() //reset timer
		rf.votedFor = args.CandidateID
		rf.persist()
		reply.VoteGranted = true
	}

	switch {
	case args.Term < rf.currentTerm:
		//outdated request
		reply.VoteGranted = false

	case args.Term >= rf.currentTerm:
		if args.Term > rf.currentTerm {
			//we are outdated
			rf.currentTerm = args.Term

			if rf.state != follower {
				rf.state = follower
			}

			rf.votedFor = -1
			rf.persist()
		}

		//normal
		if rf.votedFor != -1 && rf.votedFor != args.CandidateID { //already voted for others in this term
			reply.VoteGranted = false
			return
		}
		//check up-to-date
		switch {
		case args.LastLogTerm > rf.log.index(rf.log.lastIndex()).Term:
			grantVote()
		case args.LastLogTerm < rf.log.index(rf.log.lastIndex()).Term:
			reply.VoteGranted = false
		case args.LastLogTerm == rf.log.index(rf.log.lastIndex()).Term:
			if args.LastLogIndex >= rf.log.lastIndex() {
				grantVote()
			} else {
				reply.VoteGranted = false
			}
		}
	}
	return
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//VERY IMPORTANT:
	//this function may wait for quite a long time to return.
	//we sometimes need to resend a heartbeat before it return.
	//so this function must be executed in a seperate goroutine to achieve this.
	//see rf.leader()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.log.lastIndex() + 1
	term := rf.currentTerm
	isLeader := (rf.state == leader)

	if isLeader == false {
		//we are not the leader
		return index, term, isLeader
	}

	//start the agreement
	rf.log.Entries = append(rf.log.Entries, LogEntry{term, command})
	rf.persist()
	rf.matchIndex[rf.me] = index
	DPrintf("\nstart on leader %d\n", rf.me)
	//inform goroutines in sendLog()
	rf.newLogCome.Broadcast()

	//return immediately
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	//fmt.Printf("ticker %d start\n", rf.me)
	//ticker never exit.
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//my implementation does not use timer.Sleep()
		//All used time.Timer().

		<-rf.timer.C
		if rf.killed() {
			break
		}

		//timer fired
		//start election
		//must do election in a seperate thread
		//since election and timer has to run concurrently.
		go rf.candidate()

		rf.timerLock.Lock()
		duration := time.Duration(rand.Int())%electionTimeoutInterval + electionTimeoutStart
		rf.timer.Reset(duration)
		rf.timerLock.Unlock()
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			DPrintf("server %d admit %d.\n\n", rf.me, rf.lastApplied)
			msg := ApplyMsg{CommandValid: true, Command: rf.log.index(rf.lastApplied).Command, CommandIndex: rf.lastApplied}
			rf.mu.Unlock()
			//IMPORTANT: must **not** holding the lock while sending to applyCh.
			//OR will cause deadlock(In 2D, since Snapshot() need to hold rf.mu)!
			rf.applyCh <- msg
			rf.mu.Lock()
			DPrintf("admitted\n")
		} else {
			rf.applyCond.Wait()
		}
	}

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	// Your initialization code here (2A, 2B, 2C).

	//initialize volatile fields:
	//Volatile state on all servers:
	rf.commitIndex = 0
	rf.lastApplied = rf.log.LastIncludedIndex

	//added by me
	rf.state = follower
	rf.applyCh = applyCh

	//added in lab2B
	rf.newLogCome = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.heartbeatTimerTerminateChannel = make(chan bool)

	rand.Seed(int64(rf.me))
	// start ticker goroutine to start elections
	rf.timerLock.Lock()
	rf.timer = time.NewTimer(time.Duration(rand.Int())%electionTimeoutInterval + electionTimeoutStart)
	rf.timerLock.Unlock()
	go rf.ticker()
	//start the applier to send messages to applyCh
	go rf.applier()
	DPrintf("server %d started----------------\n", rf.me)

	return rf
}
