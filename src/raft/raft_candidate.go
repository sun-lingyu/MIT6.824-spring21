package raft

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
	getVote := 1                                         //vote for itself
	voteChannel := make(chan RequestVoteReply)           //used to get reply from sub goroutings who are requesting vote
	terminateChannel := make(chan bool, len(rf.peers)-1) //Buffered channel. To inform subgoroutines to terminate

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
			case <-terminateChannel:
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
					terminateChannel <- true
				}

				select { //this must be executed when holding lock to prevent racing with RPC handlers.
				//IMPORTANT
				//drain rf.electAbortChannel befor we abdicate leadership
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
					terminateChannel <- true
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
			if reply.VoteGranted == true { //get vote
				getVote++
				if getVote > len(rf.peers)/2 { //wins the election
					//terminate all subgoroutines
					for i := 0; i < len(rf.peers)-1; i++ { //at most len(rf.peers)-1 subgoroutines waiting.
						terminateChannel <- true
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
					go rf.leader()
					return
				}
			}
		case flag = <-electAbortChannel: //abort election
			//terminate all subgoroutines
			for i := 0; i < len(rf.peers)-1; i++ { //at most len(rf.peers)-1 subgoroutines waiting.
				terminateChannel <- true
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
		terminateChannel <- true
	}

	return
}
