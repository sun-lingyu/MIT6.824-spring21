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
	//	"bytes"

	"fmt"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

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
	log         []LogEntry

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
	electAbortChannel  chan bool
	applyCh            chan ApplyMsg
	leaderAbortChannel chan bool
	timer              *time.Timer
	timerLock          sync.Mutex
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

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
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log = make([]LogEntry, 1) //empty. (first index is one)
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//IMPORTANT
	//RPC handler do not change state itself.
	//It only send Abort messages to leader() or elect().
	//changing state is done by them
	//otherwise leader() or candidate() may be confused about their state.
	rf.mu.Lock() //hold the lock during entire handle process
	defer rf.mu.Unlock()
	defer func() { reply.Term = rf.currentTerm }()

	//fmt.Printf("server %d get heartbeat from %d. state is %d\n", rf.me, args.LeaderID, rf.state)

	switch {
	case args.Term < rf.currentTerm:
		//outdated request
		reply.Success = false
		return

	case args.Term > rf.currentTerm:
		//we are outdated
		rf.currentTerm = args.Term

		if rf.state == leader {
			//fmt.Printf("Leader %d find Outdated passively!\n", rf.me)
			go func() { rf.leaderAbortChannel <- true }() //abdicate leadership, non-blocking send
		} else if rf.state == candidate {
			//fmt.Printf("Candidate %d abdicate!\n", rf.me)
			go func() { rf.electAbortChannel <- true }() //abort election, non-blocking send
		}

	case args.Term == rf.currentTerm:
		//normal
		if rf.state == leader {
			fmt.Printf("ERROR! Another leader in current term?!") //impossible
		} else if rf.state == candidate {
			//fmt.Printf("Candidate %d abdicate!\n", rf.me)
			go func() { rf.electAbortChannel <- true }() //abort election
		}
	}

	rf.resetTimer() //reset timer

	return

	//draft code for lab 2b
	/*if args.PrevLogIndex > len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.Success = false
		return
	}

	for i, entry := range args.Entries {
		if entry.Term == rf.log[args.PrevLogIndex+i+1].Term {
			continue
		}
		//If an existing entry conflicts with a new one
		//(same index but different terms),
		//delete the existing entry and all that follow it
		rf.log = rf.log[:args.PrevLogIndex+i+1]
		for j := i; j < len(args.Entries); j++ {
			rf.log = append(rf.log, args.Entries[j])
		}
		break
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex) //index of last new entry
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
		}
	}*/

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
		//fmt.Printf("process %d vote for process %d\n", rf.me, args.CandidateID)
		rf.resetTimer() //reset timer
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	}

	switch {
	case args.Term < rf.currentTerm:
		//outdated request
		reply.VoteGranted = false

	case args.Term > rf.currentTerm:
		//we are outdated
		rf.currentTerm = args.Term

		if rf.state == leader {
			//fmt.Printf("Leader %d find Outdated passively!\n", rf.me)
			go func() { rf.leaderAbortChannel <- true }() //abdicate leadership, non-blocking send
		} else if rf.state == candidate {
			go func() { rf.electAbortChannel <- true }() //abort election, non-blocking send
		}

		//vote for him immediately
		grantVote()

	case args.Term == rf.currentTerm:
		//normal
		if rf.votedFor != -1 && rf.votedFor != args.CandidateID { //already voted for others in this term
			reply.VoteGranted = false
			return
		}
		//check up-to-date
		switch {
		case args.LastLogTerm > rf.log[len(rf.log)-1].Term:
			grantVote()
		case args.LastLogTerm < rf.log[len(rf.log)-1].Term:
			reply.VoteGranted = false
		case args.LastLogTerm == rf.log[len(rf.log)-1].Term:
			if args.LastLogIndex >= len(rf.log) {
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

func (rf *Raft) leader() {
	rf.mu.Lock()
	//fmt.Printf("leader:%d\n", rf.me)
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	heartbeatChannel := make(chan AppendEntriesReply)
	terminatChannel := make(chan bool, len(rf.peers)-1) //Buffered channel. To inform subgoroutines to terminate

	terminate := func() { //must holding the lock.
		//fmt.Printf("leader %d terminate\n", rf.me)
		for i := 0; i < len(rf.peers)-1; i++ { //at most len(rf.peers)-1 subgoroutines waiting.
			terminatChannel <- true
		}
		rf.state = follower
		rf.resetTimer() //restart ticker
	}

	//send heartbeat in parallel
	for server := range rf.peers {
		if server == rf.me { //do not send to myself
			continue
		}
		go func(server int) { //use seperate goroutines to send messages: can set independent timers.
			args := AppendEntriesArgs{Term: currentTerm, LeaderID: rf.me}
			timer := time.NewTimer(0)
			for rf.killed() == false {
				<-timer.C
				reply := AppendEntriesReply{}

				//since rf.sendAppendEntries may spend quite a lot of time before timeout, we need to stop waiting early.
				ok := false
				innerTimer := time.NewTimer(heartbeatInterval / 2) //must fire within heartbeatInterval.

				//here using two select to realize:
				//consumer listening for a while, then timeout and stop listening
				//producer goroutine won't leak
				innerChannel := make(chan bool)
				//fmt.Printf("leader %d send heartbeat to %d\n", rf.me, server)
				go func(innerChannel chan bool) {
					select {
					case innerChannel <- rf.sendAppendEntries(server, &args, &reply):
					default:
					}
				}(innerChannel)
				select {
				case ok = <-innerChannel:
				case <-innerTimer.C:
				}

				//thread leakage fixed using buffered channel: terminateChannel.
				if ok {
					select {
					case heartbeatChannel <- reply:
					case <-terminatChannel:
						return
					}
				}
				timer.Reset(heartbeatInterval)
			}
		}(server)
	}
	//fmt.Printf("leader %d receiving\n", rf.me)
	for !rf.killed() {
		select {
		case reply := <-heartbeatChannel:
			rf.mu.Lock()
			if rf.currentTerm != currentTerm { //already updated by RPC handler
				select { //this must be executed when holding lock to prevent racing with RPC handlers.
				//IMPORTANT
				//drain rf.leaderAbortChannel befor we abdicate leadership
				//otherwise may cause thread leakage
				case <-rf.leaderAbortChannel:
				default:
				}
				terminate()
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.currentTerm {
				//we are outdated
				//fmt.Printf("Leader %d find Outdated!\n", rf.me)
				select { //this must be executed when holding lock to prevent racing with RPC handlers.
				//IMPORTANT
				//drain rf.leaderAbortChannel befor we abdicate leadership
				//otherwise may cause thread leakage
				case <-rf.leaderAbortChannel:
				default:
				}
				rf.currentTerm = reply.Term
				terminate()
				rf.mu.Unlock()
				return //abdicate leadership
			}
			rf.mu.Unlock() //important! I fogot to add this sentence. It took me an hour for this!
		case <-rf.leaderAbortChannel: //abdicate leadership
			//fmt.Printf("leader %d abort\n", rf.me)
			rf.mu.Lock()
			terminate()
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) elect() {
	rf.mu.Lock()
	electAbortChannel := rf.electAbortChannel
	rf.state = candidate
	rf.currentTerm++
	//fmt.Printf("elect of process %d, term is %d\n", rf.me, rf.currentTerm)
	currentTerm := rf.currentTerm
	args := RequestVoteArgs{currentTerm, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term}
	rf.votedFor = rf.me //vote for itself
	rf.mu.Unlock()

	var flag bool = true
	getVote := 1                                        //vote for itself
	voteChannel := make(chan RequestVoteReply)          //used to get reply from sub goroutings who are requesting vote
	terminatChannel := make(chan bool, len(rf.peers)-1) //Buffered channel. To inform subgoroutines to terminate

	//request vote in parallel
	for server := range rf.peers {
		if server == rf.me { //do not send to myself
			continue
		}

		go func(server int) {
			//send only once is enough to satisfy raft rules.
			reply := RequestVoteReply{}
			rf.sendRequestVote(server, &args, &reply)

			//thread leakage fixed using buffered channel: terminateChannel.
			select {
			case voteChannel <- reply:
			case <-terminatChannel:
				return
			}
		}(server)
	}

	//listen to results
	for i := 0; i < len(rf.peers)-1; i++ { //get at most len(rf.peers)-1 replies.
		//fmt.Printf("Candidate %d listen to packet %d\n", rf.me, i)
		select {
		case reply := <-voteChannel:
			rf.mu.Lock()
			if rf.currentTerm != currentTerm { //already updated by RPC handler
				//terminate all subgoroutines
				for i := 0; i < len(rf.peers)-1; i++ { //at most len(rf.peers)-1 subgoroutines waiting.
					terminatChannel <- true
				}

				select { //this must be executed when holding lock to prevent racing with RPC handlers.
				//IMPORTANT
				//drain rf.leaderAbortChannel befor we abdicate leadership
				//otherwise may cause thread leakage
				case flag = <-electAbortChannel:
				default:
				}
				if flag { //if flag is false, means that a new election has been started. Then do not change anything.
					rf.state = follower
				}
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.currentTerm { //abort election
				//terminate all subgoroutines
				for i := 0; i < len(rf.peers)-1; i++ { //at most len(rf.peers)-1 subgoroutines waiting.
					terminatChannel <- true
				}

				select { //this must be executed when holding lock to prevent racing with RPC handlers.
				//IMPORTANT
				//drain electAbortChannel befor we abdicate candidateship
				//otherwise may cause thread leakage
				case flag = <-electAbortChannel:
				default:
				}
				if flag {
					rf.currentTerm = reply.Term
					rf.state = follower
				}
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			if reply.VoteGranted == true {
				getVote++
				if getVote > len(rf.peers)/2 { //wins the election
					//terminate all subgoroutines
					for i := 0; i < len(rf.peers)-1; i++ { //at most len(rf.peers)-1 subgoroutines waiting.
						terminatChannel <- true
					}

					select { //this must be executed when holding lock to prevent racing with RPC handlers.
					//IMPORTANT
					//drain electAbortChannel befor we abdicate candidateship
					//otherwise may cause thread leakage
					case flag = <-electAbortChannel:
					default:
					}
					if !flag {

						return
					}
					rf.mu.Lock()
					rf.state = leader //first change state, then execute leader(). To prevent race.
					rf.mu.Unlock()
					go rf.leader()
					return
				}
			}
		case flag = <-electAbortChannel: //abort election
			//terminate all subgoroutines
			for i := 0; i < len(rf.peers)-1; i++ { //at most len(rf.peers)-1 subgoroutines waiting.
				terminatChannel <- true
			}

			if flag {
				rf.mu.Lock()
				rf.state = follower
				rf.mu.Unlock()
			}
			return
		}
	}

	//Here rf.state must still be candidate,
	//since only elect() itself can really change state when rf.state==candidate
	rf.mu.Lock()
	select { //this must be executed when holding lock to prevent racing with RPC handlers.
	//IMPORTANT
	//drain rf.electAbortChannel befor we abdicate candidateship
	//otherwise may stuck!
	case <-electAbortChannel:
	default:
	}
	rf.state = follower
	rf.mu.Unlock()

	//terminate all subgoroutines
	for i := 0; i < len(rf.peers)-1; i++ { //at most len(rf.peers)-1 subgoroutines waiting.
		terminatChannel <- true
	}

	return

	//fmt.Printf("Cndidate %d exit\n", rf.me)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	//fmt.Printf("ticker %d start\n", rf.me)
	//ticker never exit.
	rf.timerLock.Lock()
	rf.timer = time.NewTimer(time.Duration(rand.Int())%electionTimeoutInterval + electionTimeoutStart)
	rf.timerLock.Unlock()
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//my implementation does not use timer.Sleep()
		//All used time.Timer().

		//select {
		//case <-rf.timer.C:
		<-rf.timer.C
		if rf.killed() {
			break
		}

		//timer fired
		//start election
		//must do election in a seperate thread
		//since election and timer has to run concurrently.
		rf.mu.Lock()
		if rf.state != leader {
			if rf.state == candidate { //a new round of election
				rf.electAbortChannel <- false          //abort last election
				rf.electAbortChannel = make(chan bool) // a new channel
			}
			go rf.elect() //begin election and restart timer
		}
		rf.mu.Unlock()

		rf.timerLock.Lock()
		duration := time.Duration(rand.Int())%electionTimeoutInterval + electionTimeoutStart
		rf.timer.Reset(duration)
		rf.timerLock.Unlock()
		//case <-rf.tickerAbortChannel:
		//	fmt.Printf("ticker %d exit\n", rf.me)
		//	return
		//}
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

	// Your initialization code here (2A, 2B, 2C).

	//initialize volatile fields:
	//Volatile state on all servers:
	rf.commitIndex = 0
	rf.lastApplied = 0

	//Volatile state on leaders:
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}

	rf.matchIndex = make([]int, len(peers)) //initialized to 0.

	//added by me
	rf.state = follower

	rf.electAbortChannel = make(chan bool)
	rf.leaderAbortChannel = make(chan bool)
	rf.applyCh = applyCh

	rand.Seed(int64(rf.me))
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
