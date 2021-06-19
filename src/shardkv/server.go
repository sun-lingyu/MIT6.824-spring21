package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
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
	Type    string
	Key     string
	Value   string
	Version int64
	ID      int
	Leader  int                       //only used for Newleader commands
	Shards  [shardctrler.NShards]bool //only used for configuration change
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
	dead            int32 // set by Kill()
	kvMap           map[string]string
	pendingChannels map[int]chan handlerReply //map from log index to channel. only valid when it is leader
	pendingMap      map[int]Op                //map from log index to cmd
	dupMap          map[int]int64             //map from client to version

	persister *raft.Persister

	mck    *shardctrler.Clerk
	shards [shardctrler.NShards]bool //shards that this group is responsible for
}

type handlerReply struct { //message between applyListener and RPC Handlers.
	err   Err
	value string //for Get.
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("server %d: process Get\n", kv.me)

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Find %d leader\n", kv.me)
	newCmd := Op{Type: "Get", Key: args.Key, Value: "get_invalid"}
	index, _, _ := kv.rf.Start(newCmd)

	kv.pendingChannels[index] = make(chan handlerReply) //used to receive Err message
	kv.pendingMap[index] = newCmd                       //used to save newCmd, for applyListener() to query.
	ch := kv.pendingChannels[index]
	kv.mu.Unlock()

	hreply := <-ch
	kv.mu.Lock()

	reply.Err = hreply.err
	reply.Value = hreply.value

	DPrintf("finish processing get: %v, Err=%v\n", reply.Value, reply.Err)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("server %d: process PutAppend\n", kv.me)

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Find %d leader\n", kv.me)

	//duplicate detection
	_, ok := kv.dupMap[args.ID]
	if !ok {
		//new client
		//initialize
		kv.dupMap[args.ID] = -1
	}
	if args.Version <= kv.dupMap[args.ID] {
		//already processed.
		reply.Err = OK
		return
	}

	newCmd := Op{Type: args.Op, Key: args.Key, Value: args.Value, Version: args.Version, ID: args.ID}
	index, _, _ := kv.rf.Start(newCmd)

	kv.pendingChannels[index] = make(chan handlerReply) //used to receive Err message
	kv.pendingMap[index] = newCmd                       //used to save newCmd, for applyListener() to query.
	ch := kv.pendingChannels[index]
	kv.mu.Unlock()

	hreply := <-ch
	kv.mu.Lock()

	reply.Err = hreply.err

	DPrintf("finish processing putappend: %v, Err=%v\n", newCmd.Value, hreply.err)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) abortLeader() {
	//we are not leader anymore.
	for i, ch := range kv.pendingChannels {
		DPrintf("%d not leader anymore!\n", kv.me)
		ch <- handlerReply{ErrWrongLeader, ""}
		close(ch)
		delete(kv.pendingChannels, i)
		delete(kv.pendingMap, i)
	}
}

func (kv *ShardKV) applyListener() {
	for !kv.killed() {
		DPrintf("%d listening\n", kv.me)
		msg := <-kv.applyCh
		DPrintf("%d get reply\n", kv.me)
		kv.mu.Lock()
		if msg.CommandValid {
			DPrintf("%d get lock in applyListener\n", kv.me)
			cmd := msg.Command.(Op)

			if cmd.Type == "Newleader" {
				if cmd.Leader == kv.me {
					go kv.pollCtrler(100 * time.Millisecond)
				} else {
					kv.abortLeader()
				}
				kv.mu.Unlock()
				continue
			}

			if cmd.Type == "Newconfig" {
				kv.shards = cmd.Shards //update shards
				kv.mu.Unlock()
				continue
			}

			//duplicate detection
			if cmd.Type != "Get" {
				_, ok := kv.dupMap[cmd.ID]
				if !ok {
					//new client
					//initialize
					kv.dupMap[cmd.ID] = -1
				}
				if cmd.Version <= kv.dupMap[cmd.ID] {
					//already processed.
					//fmt.Printf("duplicate detected\n")
					goto sendHreply
				}
				//check whether responsible for the key
				if kv.shards[key2shard(cmd.Key)] == false {
					goto sendHreply
				}
				//not a duplicate one
				//must **not** treat irresponsible ones as duplicate
				//since we may be outdated compared to the ctrler, and we are actually responsible
				//then the client will retry and send the same cmd to us
				//we should not treat such cmds as duplicate!
				kv.dupMap[cmd.ID] = cmd.Version
				//apply changes
				switch cmd.Type {
				case "Append":
					kv.kvMap[cmd.Key] = kv.kvMap[cmd.Key] + cmd.Value
				case "Put":
					kv.kvMap[cmd.Key] = cmd.Value
				default:
					panic(fmt.Sprintf("applyListener: wrong cmd.Type: %s!\n", cmd.Type))
				}
			}

		sendHreply:
			_, hasKey := kv.pendingChannels[msg.CommandIndex]
			oldCmd := kv.pendingMap[msg.CommandIndex]

			if hasKey {
				if cmd != oldCmd {
					kv.abortLeader()
				} else {
					DPrintf("%d send out of loop\n", kv.me)
					var hreply handlerReply
					//check whether responsible for the key
					if kv.shards[key2shard(cmd.Key)] == false {
						hreply.err = ErrWrongGroup
					} else {
						if cmd.Type == "Get" {
							elem, ok := kv.kvMap[cmd.Key]
							if ok {
								hreply.err = OK
								hreply.value = elem
							} else {
								hreply.err = ErrNoKey
							}
						} else {
							//Put or Append
							hreply.err = OK
						}
					}

					kv.pendingChannels[msg.CommandIndex] <- hreply
					close(kv.pendingChannels[msg.CommandIndex])
					delete(kv.pendingChannels, msg.CommandIndex)
					delete(kv.pendingMap, msg.CommandIndex)
				}
				DPrintf("%d sending finish\n", kv.me)
			} else { //must exist another leader.
				kv.abortLeader()
			}

			//check whether need to snapshot.
			if kv.maxraftstate != -1 && float32(kv.persister.RaftStateSize()) > float32(kv.maxraftstate)*0.95 {
				//fmt.Printf("server:%d, msg.index: %d, before: %d\n", kv.me, msg.CommandIndex, kv.persister.RaftStateSize())
				//fmt.Printf("cmd.Value:%v\n", cmd.Value)
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.dupMap)
				e.Encode(kv.kvMap)
				data := w.Bytes()
				kv.rf.Snapshot(msg.CommandIndex, data)
				//fmt.Printf("server:%d, msg.index: %d, after: %d\n", kv.me, msg.CommandIndex, kv.persister.RaftStateSize())
			}

		} else if msg.SnapshotValid {
			//snapshot
			//fmt.Printf("server:%d, SnapshotIndex: %d, condinstall begin\n", kv.me, msg.SnapshotIndex)
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.readPersist(msg.Snapshot)
			}
			//fmt.Printf("server:%d, SnapshotIndex: %d,condinstall finish\n", kv.me, msg.SnapshotIndex)
		} else {
			newCmd := Op{Type: "Newleader", Key: "Newleader_invalid", Value: "Newleader_invalid", Leader: kv.me} //dummy cmd
			kv.rf.Start(newCmd)
		}
		kv.mu.Unlock()

	}
}

func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var dupMap map[int]int64
	var kvMap map[string]string
	if d.Decode(&dupMap) != nil ||
		d.Decode(&kvMap) != nil {
		fmt.Printf("server %d readPersist(kv): decode error!", kv.me)
	} else {
		kv.dupMap = dupMap
		kv.kvMap = kvMap
	}
}

func (kv *ShardKV) askMissing(map[int][]int) {

}

//only leader can poll
func (kv *ShardKV) pollCtrler(duration time.Duration) {
	configNum := 1
	var currshards [shardctrler.NShards]bool //initialized to all false
	var currconfig shardctrler.Config
	for !kv.killed() {

		//check leadership
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

		//query for new configuration
		newConfig := kv.mck.Query(configNum)
		if newConfig.Num != configNum {
			//get an old config
			time.Sleep(duration)
			continue
		}

		//get a new config
		kv.mu.Lock() //REMEMBER TO UNLOCK!!!
		configNum++

		//parse the config
		//newshards that need to be fetched from other groups
		newshards := make(map[int][]int) //gid->shards
		if configNum != 2 {
			//not needed if this is the first valid config(i.e. configNum==2)
			for shard, gid := range newConfig.Shards {
				if gid == kv.gid && currshards[shard] == false {
					originalGid := currconfig.Shards[shard]
					_, ok := newshards[originalGid]
					if !ok {
						newshards[originalGid] = make([]int, 0)
					}
					newshards[originalGid] = append(newshards[originalGid], shard)
				}
			}
		}
		//update currshards
		for shard, gid := range newConfig.Shards {
			if gid == kv.gid {
				currshards[shard] = true
				//fmt.Printf("gid %d is responsible for shard %d\n", gid, shard)
			} else {
				currshards[shard] = false
			}
		}

		//ask for missing shards synchronously
		if configNum != 2 {
			//not needed if this is the first valid config(i.e. configNum==2)
			kv.askMissing(newshards)
		}

		//acceptable to continue to store shards that it no longer owns
		//a possible optimization is to discard shards when all groups have advanced above those shards' configNum
		//but it is built on this, and we are not required to do so.

		newCmd := Op{Type: "Newconfig", Key: "Newconfig_invalid", Value: "Newconfig_invalid", Shards: currshards}
		kv.rf.Start(newCmd)

		kv.mu.Unlock()
		time.Sleep(duration)
	}
}

//
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
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.pendingChannels = make(map[int]chan handlerReply)
	kv.pendingMap = make(map[int]Op)
	kv.dupMap = make(map[int]int64)

	kv.persister = persister

	kv.readPersist(persister.ReadSnapshot())

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	go kv.applyListener()

	return kv
}
