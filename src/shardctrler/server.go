package shardctrler

import (
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead int32 // set by Kill()

	pendingChannels map[int]chan bool //log index -> channel. only valid when it is leader
	pendingMap      map[int]Op        //log index -> cmd
	dupMap          map[int]int64     //client ID -> version

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Type    string
	Version int64
	ID      int
	Leader  int //only used for Newleader commands
	JArgs   JoinArgs
	LArgs   LeaveArgs
	MArgs   MoveArgs
	QArgs   QueryArgs
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()

	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	_, ok := sc.dupMap[args.ID]
	if !ok {
		//new client
		//initialize
		sc.dupMap[args.ID] = -1
	}
	if args.Version <= sc.dupMap[args.ID] {
		//already processed.
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	newCmd := Op{"Join", args.Version, args.ID, -1, *args, LeaveArgs{}, MoveArgs{}, QueryArgs{}}
	index, _, _ := sc.rf.Start(newCmd)

	sc.pendingChannels[index] = make(chan bool) //used to receive Err message
	sc.pendingMap[index] = newCmd               //used to save newCmd, for applyListener() to query.
	ch := sc.pendingChannels[index]
	sc.mu.Unlock()

	success := <-ch

	if success {
		reply.Err = OK
	} else {
		reply.Err = WRONG
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()

	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	_, ok := sc.dupMap[args.ID]
	if !ok {
		//new client
		//initialize
		sc.dupMap[args.ID] = -1
	}
	if args.Version <= sc.dupMap[args.ID] {
		//already processed.
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	newCmd := Op{"Leave", args.Version, args.ID, -1, JoinArgs{}, *args, MoveArgs{}, QueryArgs{}}
	index, _, _ := sc.rf.Start(newCmd)

	sc.pendingChannels[index] = make(chan bool) //used to receive Err message
	sc.pendingMap[index] = newCmd               //used to save newCmd, for applyListener() to query.
	ch := sc.pendingChannels[index]
	sc.mu.Unlock()

	success := <-ch

	if success {
		reply.Err = OK
	} else {
		reply.Err = WRONG
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()

	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	_, ok := sc.dupMap[args.ID]
	if !ok {
		//new client
		//initialize
		sc.dupMap[args.ID] = -1
	}
	if args.Version <= sc.dupMap[args.ID] {
		//already processed.
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	newCmd := Op{"Move", args.Version, args.ID, -1, JoinArgs{}, LeaveArgs{}, *args, QueryArgs{}}
	index, _, _ := sc.rf.Start(newCmd)

	sc.pendingChannels[index] = make(chan bool) //used to receive Err message
	sc.pendingMap[index] = newCmd               //used to save newCmd, for applyListener() to query.
	ch := sc.pendingChannels[index]
	sc.mu.Unlock()

	success := <-ch

	if success {
		reply.Err = OK
	} else {
		reply.Err = WRONG
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()

	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	newCmd := Op{"Query", args.Version, args.ID, -1, JoinArgs{}, LeaveArgs{}, MoveArgs{}, *args}
	index, _, _ := sc.rf.Start(newCmd)

	sc.pendingChannels[index] = make(chan bool) //used to receive Err message
	sc.pendingMap[index] = newCmd               //used to save newCmd, for applyListener() to query.
	ch := sc.pendingChannels[index]
	sc.mu.Unlock()

	success := <-ch

	if success {
		reply.Err = OK
		sc.mu.Lock()
		if args.Num == -1 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		sc.mu.Unlock()
	} else {
		reply.Err = WRONG
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applyListener() {
	for !sc.killed() {
		msg := <-sc.applyCh
		sc.mu.Lock()

		if msg.CommandValid {

			cmd := msg.Command.(Op)

			if cmd.Type == "Newleader" {
				if cmd.Leader == sc.me {
					sc.mu.Unlock()
					continue
				}
				for i, ch := range sc.pendingChannels {
					//we are not leader anymore.
					ch <- false
					close(ch)
					delete(sc.pendingChannels, i)
					delete(sc.pendingMap, i)
				}
			}

			//duplicate detection
			if cmd.Type != "Query" {
				_, ok := sc.dupMap[cmd.ID]
				if !ok {
					//new client
					//initialize
					sc.dupMap[cmd.ID] = -1
				}
				if cmd.Version <= sc.dupMap[cmd.ID] {
					//already processed.
					//fmt.Printf("duplicate detected\n")
					goto sendHreply
				}

				//apply changes
				length := len(sc.configs)
				//initialize new config as sc.configs[length-1]
				newConfig := Config{}
				newConfig.Num = sc.configs[length-1].Num + 1
				newConfig.Shards = sc.configs[length-1].Shards //assignment between arrays is deepcopy.
				newConfig.Groups = make(map[int][]string)
				for gid, servers := range sc.configs[length-1].Groups {
					newConfig.Groups[gid] = servers
				}

				switch cmd.Type {
				case "Join":
					for gid, servers := range cmd.JArgs.Servers {
						newConfig.Groups[gid] = servers
					}
					avg := NShards / len(newConfig.Groups)

					counter := make(map[int]int)
					for gid, _ := range newConfig.Groups {
						counter[gid] = 0
					}

					for shard, gid := range newConfig.Shards {
						if gid == 0 || counter[gid] >= avg {
							targetGid := findArgMin(counter)
							newConfig.Shards[shard] = targetGid
							counter[targetGid]++
						} else if gid != 0 {
							counter[gid]++
						}
					}
				case "Leave":
					for _, gid := range cmd.LArgs.GIDs {
						delete(newConfig.Groups, gid)
					}
					var avg int
					if len(newConfig.Groups) == 0 {
						avg = NShards
					} else {
						avg = NShards / len(newConfig.Groups)
					}

					counter := make(map[int]int)
					for gid, _ := range newConfig.Groups {
						counter[gid] = 0
					}

					leave := make(map[int]int)
					for _, gid := range cmd.LArgs.GIDs {
						leave[gid] = 0
					}

					for shard, gid := range newConfig.Shards {
						_, ok := leave[gid]
						//TODO: rebalance
						if ok {
							//moved
							if len(counter) > 0 {
								targetGid := findArgMin(counter)
								newConfig.Shards[shard] = targetGid
								counter[targetGid]++
							} else {
								newConfig.Shards[shard] = 0
							}
						} else {
							//not moved
							if counter[gid] >= avg {
								targetGid := findArgMin(counter)
								newConfig.Shards[shard] = targetGid
								counter[targetGid]++
							} else {
								counter[gid]++
							}
						}
					}
				case "Move":
					newConfig.Shards[cmd.MArgs.Shard] = cmd.MArgs.GID
				}
				sc.configs = append(sc.configs, newConfig)
			}

		sendHreply:
			_, hasKey := sc.pendingChannels[msg.CommandIndex]
			oldCmd := sc.pendingMap[msg.CommandIndex]

			if hasKey {
				if cmd.Version != oldCmd.Version {
					for i, ch := range sc.pendingChannels {
						//clear all
						ch <- false
						close(ch)
						delete(sc.pendingChannels, i)
						delete(sc.pendingMap, i)
					}
				} else {
					sc.pendingChannels[msg.CommandIndex] <- true
					close(sc.pendingChannels[msg.CommandIndex])
					delete(sc.pendingChannels, msg.CommandIndex)
					delete(sc.pendingMap, msg.CommandIndex)
				}
			} else {
				for i, ch := range sc.pendingChannels {
					//clear all
					ch <- false
					close(ch)
					delete(sc.pendingChannels, i)
					delete(sc.pendingMap, i)
				}
			}

		} else {
			newCmd := Op{"Newleader", -1, -1, sc.me, JoinArgs{}, LeaveArgs{}, MoveArgs{}, QueryArgs{}} //dummy cmd
			sc.rf.Start(newCmd)
		}

		sc.mu.Unlock()

	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.pendingChannels = make(map[int]chan bool)
	sc.pendingMap = make(map[int]Op)
	sc.dupMap = make(map[int]int64)

	go sc.applyListener()

	return sc
}
