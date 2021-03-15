package raft

import (
	"fmt"
	"time"
)

func (rf *Raft) sender(args AppendEntriesArgs, server int, replyChannel chan AppendEntriesReply, terminateChannel chan bool) (bool, AppendEntriesReply) {
	reply := AppendEntriesReply{}

	//since rf.sendAppendEntries may spend quite a lot of time before timeout, we need to stop waiting early.
	ok := false
	innerTimer := time.NewTimer(heartbeatInterval) //must fire within heartbeatInterval.

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
		return ok, reply
	case <-innerTimer.C:
		return false, AppendEntriesReply{}
	}
	//thread leakage fixed using buffered channel: terminateChannel.
	/*if ok {
		fmt.Printf("server %d sent\n", server)
		select {
		case replyChannel <- reply:
		case <-terminateChannel:
			return false
		}
	}
	return true*/
}

func (rf *Raft) receiver(args AppendEntriesArgs, reply AppendEntriesReply, currentTerm int, terminateChannel chan bool, server int) bool {
	if rf.checkAppendEntriesReply(reply, currentTerm, terminateChannel) == false {
		return false
	}

	if reply.Success {
		rf.mu.Lock()
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = findKthLargest(rf.matchIndex, len(rf.peers)/2+1)
		DPrintf("leader matchIndex: %v\n", rf.matchIndex)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			DPrintf("server %d admit %d %d.\n\n", rf.me, rf.log[rf.lastApplied].Command, rf.lastApplied)
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
		}
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		rf.nextIndex[server]--
		if rf.nextIndex[server] < 1 {
			fmt.Printf("ERROR: rf.nextIndex[server] < 1 (=%d)\n", rf.nextIndex[server])
		}
		rf.mu.Unlock()
	}
	return true
}

func (rf *Raft) leaderProcess(currentTerm int, replyChannel chan AppendEntriesReply, terminateChannel chan bool) {
	//transient function.
	//start many goroutines to send heartbeat in parallel
	for server := range rf.peers {
		if server == rf.me { //do not send to myself
			continue
		}
		go func(server int) { //use seperate goroutines to send messages: can set independent timers.
			//initial heartbeat.
			rf.mu.Lock()
			prevLogIndex := rf.nextIndex[server] - 1 //IMPORTANT: prevLogIndex, not lastLogEntryIndex!
			prevLogTerm := rf.log[prevLogIndex].Term
			args := AppendEntriesArgs{Term: currentTerm, LeaderID: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: []LogEntry{}, LeaderCommit: rf.commitIndex}
			rf.mu.Unlock()
			DPrintf("leader %d send heartbeat to %d\n", rf.me, server)
			ok, reply := rf.sender(args, server, replyChannel, terminateChannel)
			if ok {
				if rf.receiver(args, reply, currentTerm, terminateChannel, server) == false {
					return
				}
			}

			for !rf.killed() {
				//each loop: send all available log entries available and ensure success.

				//check: idle
				rf.newLogCome.L.Lock()
				rf.mu.Lock()
				lastLogEntryIndex := len(rf.log) - 1
				//if leader is idle, then it should wait until new log entry comes or timer fire.
				for rf.nextIndex[server] > lastLogEntryIndex {
					//if it wakes up and find still idle,
					//then it must be woken up by heartBeatChannel,
					//should send heartbeat.
					prevLogIndex := rf.nextIndex[server] - 1 //IMPORTANT: prevLogIndex, not lastLogEntryIndex!(though equal here.)
					prevLogTerm := rf.log[prevLogIndex].Term
					args := AppendEntriesArgs{Term: currentTerm, LeaderID: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: []LogEntry{}, LeaderCommit: rf.commitIndex}
					rf.mu.Unlock()
					DPrintf("leader %d send heartbeat to %d\n", rf.me, server)
					ok, reply = rf.sender(args, server, replyChannel, terminateChannel)
					if ok {
						if rf.receiver(args, reply, currentTerm, terminateChannel, server) == false {
							return
						}
					}
					rf.newLogCome.Wait()
					select {
					case <-terminateChannel:
						rf.newLogCome.L.Unlock()
						return
					default:
					}
					rf.mu.Lock()
					//when wake up, lastLogEntryIndex may have changed.
					lastLogEntryIndex = len(rf.log) - 1
				}
				rf.newLogCome.L.Unlock()

				//not idle
				//still in rf.mu.Lock()
				prevLogIndex := rf.nextIndex[server] - 1
				prevLogTerm := rf.log[prevLogIndex].Term
				lastLogEntryIndex = len(rf.log) - 1
				for !rf.killed() && rf.nextIndex[server] <= lastLogEntryIndex {
					select {
					case <-terminateChannel:
						rf.mu.Unlock()
						return
					default:
					}
					prevLogIndex = rf.nextIndex[server] - 1
					prevLogTerm = rf.log[prevLogIndex].Term
					args := AppendEntriesArgs{Term: currentTerm, LeaderID: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: rf.log[prevLogIndex+1:], LeaderCommit: rf.commitIndex}
					rf.mu.Unlock()
					ok, reply = rf.sender(args, server, replyChannel, terminateChannel)
					if ok {
						if rf.receiver(args, reply, currentTerm, terminateChannel, server) == false {
							return
						}
					}
					rf.mu.Lock()
					lastLogEntryIndex = len(rf.log) - 1
					//MAYBE NEED TO WAIT FOR A WHILE TO GIVE SOME TIME FOR THE RECEIVER(leader()) TO CHANGE THE STATE
				}
				rf.mu.Unlock()
			}

		}(server)
	}
}

/*func (rf *Raft) drainChannel(target chan bool) {
	for {
		select {
		case <-target:
		default:
			return
		}
	}
}*/

func (rf *Raft) checkAppendEntriesReply(reply AppendEntriesReply, currentTerm int, terminateChannel chan bool) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != currentTerm { //already updated by RPC handler
		//TODO: double check the logic here is right.
		select {
		case <-terminateChannel:
			return false
		default:
		}

		rf.leaderAbortCond.Signal()

		return false
	}
	if reply.Term > rf.currentTerm {
		//we are outdated
		rf.currentTerm = reply.Term
		//TODO: double check the logic here is right.
		select {
		case <-terminateChannel:
			return false
		default:
		}

		rf.leaderAbortCond.Signal()

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
		rf.nextIndex[server] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers)) //initialized to 0.

	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	replyChannel := make(chan AppendEntriesReply)
	terminateChannel := make(chan bool, len(rf.peers)-1) //Buffered channel. To inform subgoroutines to terminate
	heartbeatTimerTerminateChannel := make(chan bool)

	terminate := func() { //must holding the lock.
		//fmt.Printf("leader %d terminate\n", rf.me)
		for i := 0; i < len(rf.peers)-1; i++ { //at most len(rf.peers)-1 subgoroutines waiting.
			terminateChannel <- true
		}
		heartbeatTimerTerminateChannel <- true
		rf.state = follower
		rf.resetTimer() //restart ticker
	}

	//set a timer for all subgoroutines created by rf.leaderProcess(to send heartbeat).
	heartbeatTimer := time.NewTimer(heartbeatInterval)
	go func() {
		for {
			select {
			case <-heartbeatTimer.C:
				rf.newLogCome.Broadcast()
				heartbeatTimer.Reset(heartbeatInterval)
			case <-heartbeatTimerTerminateChannel:
				rf.newLogCome.Broadcast()
				return
			}
		}
	}()

	//start len(rf.peers) subgoroutines to handle leader job seperately.
	rf.leaderProcess(currentTerm, replyChannel, terminateChannel)

	//listen to leaderAbortChannel
	rf.leaderAbortCond.L.Lock()
	rf.leaderAbortCond.Wait()
	rf.leaderAbortCond.L.Unlock()
	//fmt.Printf("\n\nleader quit\n\n")

	rf.mu.Lock()
	terminate()
	rf.mu.Unlock()

}
