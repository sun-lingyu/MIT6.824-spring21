package raft

import (
	"fmt"
	"time"
)

func (rf *Raft) sender_snapshot(args InstallSnapshotArgs, currentTerm int, server int) {
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if ok && !rf.killed() {
		DPrintf("leader %d receive sendInstallSnapshot reply from %d\n", rf.me, server)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//defer fmt.Printf("server %d sender exit, ok==true\n", rf.me)
		if currentTerm != rf.currentTerm || rf.state != leader || rf.checkInstallSnapshotReply(reply, currentTerm) == false {
			return
		}
		rf.nextIndex[server] = args.LastIncludedIndex + 1
	}
}

func (rf *Raft) sender(args AppendEntriesArgs, currentTerm int, server int) {
	//fmt.Printf("server %d begin sending to server %d, with %d log entrys in args \n", rf.me, server, len(args.Entries))
	//fmt.Printf("curently %d goroutines.\n", runtime.NumGoroutine())
	reply := AppendEntriesReply{}
	//start := time.Now()

	ok := rf.sendAppendEntries(server, &args, &reply)

	//elapsed := time.Since(start)
	//log.Printf("server %d received reply from server %d, took %s", rf.me, server, elapsed)
	if ok && !rf.killed() {
		DPrintf("leader %d receive from %d\n", rf.me, server)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//defer fmt.Printf("server %d sender exit, ok==true\n", rf.me)
		if currentTerm != rf.currentTerm || rf.state != leader || rf.checkAppendEntriesReply(reply, currentTerm) == false {
			return
		}
		rf.receiver(args, reply, currentTerm, server)
	}
	//fmt.Printf("server %d sender exit, ok==false\n", rf.me)
}

func (rf *Raft) receiver(args AppendEntriesArgs, reply AppendEntriesReply, currentTerm int, server int) {
	//must be holding the lock
	if reply.Success {
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		majorityMatchIndex := findKthLargest(rf.matchIndex, len(rf.peers)/2+1)
		if majorityMatchIndex > rf.commitIndex && rf.log.index(majorityMatchIndex).Term == rf.currentTerm {
			rf.commitIndex = majorityMatchIndex
			DPrintf("leader matchIndex: %v\n", rf.matchIndex)
			if rf.commitIndex > rf.lastApplied {
				rf.applyCond.Signal()
			}
		}

	} else {
		//implement optimization mentioned in the paper
		tmp := rf.nextIndex[server]
		if reply.ConflictTermFirstIndex == -1 {
			//reply to outdated request
			return
		}
		if reply.ConflictEntryTerm == -1 {
			//follower's log shorter than rf.nextIndex[server]
			rf.nextIndex[server] = reply.ConflictTermFirstIndex
		} else if reply.ConflictEntryTerm < args.PrevLogTerm {
			//go back to last term of the leader
			for rf.nextIndex[server] > rf.log.LastIncludedIndex+1 && rf.log.index(rf.nextIndex[server]-1).Term == args.PrevLogTerm {
				rf.nextIndex[server]--
			}
			if rf.nextIndex[server] != 1 && rf.nextIndex[server] == rf.log.LastIncludedIndex+1 && rf.log.LastIncludedTerm == args.PrevLogTerm {
				//special case:
				//we are hitting the snapshot.
				rf.nextIndex[server]--
			}
		} else {
			//reply.ConflictEntryTerm > args.PrevLogTerm
			//go back to last term of the follower
			rf.nextIndex[server] = reply.ConflictTermFirstIndex
		}
		DPrintf("rf.nextIndex[server] decreased: %d\n", rf.nextIndex[server])

		if rf.nextIndex[server] < 1 {
			fmt.Printf("ERROR: rf.nextIndex[server] < 1 (=%d)\n", tmp-rf.nextIndex[server])
		}
	}
}

func (rf *Raft) leaderProcess(currentTerm int) {
	//transient function.
	//start many goroutines to send heartbeat in parallel
	for server := range rf.peers {
		if server == rf.me { //do not send to myself
			continue
		}
		prevLogIndex := rf.nextIndex[server] - 1 //IMPORTANT: prevLogIndex, not lastLogEntryIndex!
		prevLogTerm := rf.log.index(prevLogIndex).Term
		args := AppendEntriesArgs{Term: currentTerm, LeaderID: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: []LogEntry{}, LeaderCommit: rf.commitIndex}
		go func(server int) { //use seperate goroutines to send messages: can set independent timers.
			//initial heartbeat.
			DPrintf("leader %d send heartbeat to %d\n", rf.me, server)
			go rf.sender(args, currentTerm, server)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for !rf.killed() {
				if rf.currentTerm != currentTerm && rf.state != leader {
					return
				}

				//each loop: send all available log entries available and ensure success.

				//if leader is idle, then it should wait until new log entry comes or timer fire.
				for !rf.killed() && rf.nextIndex[server] > rf.log.lastIndex() {
					if rf.currentTerm != currentTerm && rf.state != leader {
						return
					}
					//if it wakes up and find still idle,
					//then it must be woken up by heartBeatChannel,
					//should send heartbeat.
					prevLogIndex := rf.nextIndex[server] - 1 //IMPORTANT: prevLogIndex, not lastLogEntryIndex!(though equal here.)
					prevLogTerm := rf.log.index(prevLogIndex).Term
					args := AppendEntriesArgs{Term: currentTerm, LeaderID: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: []LogEntry{}, LeaderCommit: rf.commitIndex}
					DPrintf("leader %d send heartbeat to %d\n", rf.me, server)
					go rf.sender(args, currentTerm, server)
					rf.newLogCome.Wait()
				}

				//not idle
				//still in rf.mu.Lock()
				for !rf.killed() && rf.nextIndex[server] <= rf.log.lastIndex() {
					if rf.currentTerm != currentTerm && rf.state != leader {
						return
					}
					prevLogIndex = rf.nextIndex[server] - 1
					if prevLogIndex < rf.log.LastIncludedIndex {
						//should send snapshot
						args := InstallSnapshotArgs{Term: currentTerm, LeaderID: rf.me, LastIncludedIndex: rf.log.LastIncludedIndex, LastIncludedTerm: rf.log.LastIncludedTerm, Data: rf.snapshot}
						go rf.sender_snapshot(args, currentTerm, server)
					} else {
						prevLogTerm := rf.log.index(prevLogIndex).Term
						//should send normal appendentries RPC.
						args := AppendEntriesArgs{Term: currentTerm, LeaderID: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: append([]LogEntry(nil), rf.log.Entries[prevLogIndex-rf.log.LastIncludedIndex:]...), LeaderCommit: rf.commitIndex}
						go rf.sender(args, currentTerm, server)
					}
					rf.mu.Unlock()
					time.Sleep(heartbeatInterval / 2) //wait for rf.sender to get reply and process it
					rf.mu.Lock()
				}
			}
		}(server)
	}
}

func (rf *Raft) checkAppendEntriesReply(reply AppendEntriesReply, currentTerm int) bool {
	if reply.Term > rf.currentTerm {
		//we are outdated
		rf.heartbeatTimerTerminateChannel <- true
		rf.state = follower
		rf.currentTerm = reply.Term
		rf.persist()
		rf.resetTimer() //restart ticker
		return false
	}
	return true
}
func (rf *Raft) checkInstallSnapshotReply(reply InstallSnapshotReply, currentTerm int) bool {
	if reply.Term > rf.currentTerm {
		//we are outdated
		rf.heartbeatTimerTerminateChannel <- true
		rf.state = follower
		rf.currentTerm = reply.Term
		rf.persist()
		rf.resetTimer() //restart ticker
		return false
	}
	return true
}

func (rf *Raft) leader() {
	DPrintf("leader:%d\n", rf.me)

	rf.mu.Lock()
	rf.state = leader
	//Volatile state on leaders:
	rf.nextIndex = make([]int, len(rf.peers))
	for server := range rf.nextIndex {
		rf.nextIndex[server] = rf.log.lastIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers)) //initialized to 0.

	currentTerm := rf.currentTerm

	//set a timer for all subgoroutines created by rf.leaderProcess(to send heartbeat).
	heartbeatTimer := time.NewTimer(heartbeatInterval)
	go func() {
		for {
			select {
			case <-heartbeatTimer.C:
				rf.newLogCome.Broadcast()
				heartbeatTimer.Reset(heartbeatInterval)
			case <-rf.heartbeatTimerTerminateChannel:
				rf.newLogCome.Broadcast()
				return
			}
		}
	}()

	//start len(rf.peers) subgoroutines to handle leader job seperately.
	rf.leaderProcess(currentTerm)
	rf.mu.Unlock()

}
