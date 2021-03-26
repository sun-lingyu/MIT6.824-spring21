package raft

func (rf *Raft) checkRequestVote(reply RequestVoteReply, currentTerm int) bool {
	if reply.Term > rf.currentTerm {
		//we are outdated
		rf.currentTerm = reply.Term
		rf.persist()
		rf.state = follower

		return false
	}
	return true
}

func (rf *Raft) candidateProcess(args RequestVoteArgs, currentTerm int) {
	//request vote in parallel
	for server := range rf.peers {
		if server == rf.me { //do not send to myself
			continue
		}

		go func(server int) {
			//send only once is enough to satisfy raft rules.
			reply := RequestVoteReply{}
			DPrintf("server %d send requestvote to %d\n", rf.me, server)
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				DPrintf("server %d receive requestvote from %d\n", rf.me, server)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if currentTerm != rf.currentTerm || rf.state != candidate || rf.getVote >= len(rf.peers)/2+1 || rf.checkRequestVote(reply, currentTerm) == false {
					return
				}
				DPrintf("server %d receive requestvote from %d: passed check\n", rf.me, server)
				if reply.VoteGranted {
					DPrintf("server %d receive requestvote from %d: vote granted\n", rf.me, server)
					rf.getVote++
					if rf.getVote == len(rf.peers)/2+1 {
						go rf.leader()
					}
				}
			}
		}(server)
	}
}

func (rf *Raft) candidate() {
	rf.mu.Lock()
	if rf.state == leader { //check if leader
		rf.mu.Unlock()
		return
	}

	rf.currentTerm++
	rf.state = candidate

	//fmt.Printf("elect of process %d, term is %d\n", rf.me, rf.currentTerm)
	currentTerm := rf.currentTerm
	args := RequestVoteArgs{currentTerm, rf.me, rf.log.lastIndex(), rf.log.index(rf.log.lastIndex()).Term}
	rf.votedFor = rf.me //vote for itself
	rf.persist()
	rf.getVote = 1
	rf.mu.Unlock()

	//start len(rf.peers) subgoroutines to handle leader job seperately.
	rf.candidateProcess(args, currentTerm)
}
