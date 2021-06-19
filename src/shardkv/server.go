package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"strconv"
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
	Leader  int //only used for Newleader commands
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
	newCmd := Op{"Get", args.Key, "get_invalid", -1, -1, -1}
	index, _, _ := kv.rf.Start(newCmd)

	kv.pendingChannels[index] = make(chan handlerReply) //used to receive Err message
	kv.pendingMap[index] = newCmd                       //used to save newCmd, for applyListener() to query.
	ch := kv.pendingChannels[index]
	kv.mu.Unlock()

	hreply := <-ch
	kv.mu.Lock()

	reply.Err = hreply.err
	reply.Value = hreply.value
	if hreply.err == OK {
		fmt.Printf("Get returned %v\n", hreply.value)
	}

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

	newCmd := Op{args.Op, args.Key, args.Value, args.Version, args.ID, -1}
	index, _, _ := kv.rf.Start(newCmd)

	kv.pendingChannels[index] = make(chan handlerReply) //used to receive Err message
	kv.pendingMap[index] = newCmd                       //used to save newCmd, for applyListener() to query.
	ch := kv.pendingChannels[index]
	kv.mu.Unlock()

	hreply := <-ch
	kv.mu.Lock()

	reply.Err = hreply.err
	if args.Key == strconv.Itoa(0) {
		fmt.Printf("%v\n", hreply.value)
	}

	if hreply.err == OK {
		fmt.Printf("%v PutAppend returned %v\n", args.Key, hreply.value)
	}

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
					kv.mu.Unlock()
					continue
				}
				for i, ch := range kv.pendingChannels {
					//we are not leader anymore.
					DPrintf("%d not leader anymore!\n", kv.me)
					ch <- handlerReply{ErrWrongLeader, ""}
					close(ch)
					delete(kv.pendingChannels, i)
					delete(kv.pendingMap, i)
				}
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
				//not a duplicate one
				kv.dupMap[cmd.ID] = cmd.Version
				//check whether responsible for the key
				if kv.shards[key2shard(cmd.Key)] == false {
					goto sendHreply
				}
				//apply changes
				fmt.Printf("%v = %v applied!\n", cmd.Key, cmd.Value)
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
					for i, ch := range kv.pendingChannels {
						//clear all
						DPrintf("%d send in loop\n", kv.me)
						ch <- handlerReply{ErrWrongLeader, ""}
						close(ch)
						delete(kv.pendingChannels, i)
						delete(kv.pendingMap, i)
					}

				} else {
					DPrintf("%d send out of loop\n", kv.me)
					var hreply handlerReply
					//check whether responsible for the key
					if kv.shards[key2shard(cmd.Key)] == false {
						hreply.err = ErrWrongGroup
					} else {
						if cmd.Type == "Get" {
							elem, ok := kv.kvMap[cmd.Key]
							fmt.Printf("%v, %v, %v\n", cmd.Key, ok, elem)
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
				for i, ch := range kv.pendingChannels {
					//clear all
					DPrintf("%d send in loop1\n", kv.me)
					ch <- handlerReply{ErrWrongLeader, ""}
					close(ch)
					delete(kv.pendingChannels, i)
					delete(kv.pendingMap, i)
				}
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
			newCmd := Op{"Newleader", "Newleader_invalid", "Newleader_invalid", -1, -1, kv.me} //dummy cmd
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

func (kv *ShardKV) pollCtrler(duration time.Duration) {
	configNum := 1
	for !kv.killed() {
		newConfig := kv.mck.Query(configNum)
		if newConfig.Num != configNum {
			//get an old config
			time.Sleep(duration)
			continue
		}
		//fmt.Printf("Get new config %d, I'm %d in group %d\n", configNum, kv.me, kv.gid)
		//get a new config
		//fmt.Printf("%v", newConfig.Shards)
		kv.mu.Lock() //REMEMBER TO UNLOCK!!!
		configNum++

		for shard, gid := range newConfig.Shards {
			if gid == kv.gid {
				kv.shards[shard] = true
				//fmt.Printf("gid %d is responsible for shard %d\n", gid, shard)
			} else {
				kv.shards[shard] = false
			}
		}
		kv.mu.Unlock()
		time.Sleep(duration)

		//check: first valid config
		/*if configNum == 2 {
			for shard, gid := range newConfig.Shards {
				if gid == kv.gid {
					kv.shards[shard] = true
					//fmt.Printf("gid %d is responsible for shard %d\n", gid, shard)
				}
			}
			kv.mu.Unlock()
			time.Sleep(duration)
			continue
		}*/

		//TODO: complete the following logic.

		//parse the config
		/*newshards := make([]int, 0)
		oldshards := make([]int, 0)
		for shard, gid := range newConfig.Shards {
			if gid == kv.gid && kv.shards[shard] == false {
				newshards = append(newshards, shard)
			}
		}
		for shard, v := range kv.shards {
			if v && newConfig.Shards[shard] != kv.gid {
				oldshards = append(oldshards, shard)
			}
		}

		//request newshards

		kv.mu.Unlock()*/
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

	go kv.pollCtrler(100 * time.Millisecond)

	return kv
}
