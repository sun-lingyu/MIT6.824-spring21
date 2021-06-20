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
	Type          string
	Key           string
	Value         string
	Version       int64
	ID            int
	Leader        int                       //only used for Newleader commands
	Shards        [shardctrler.NShards]bool //only used for Newconfig commands
	NewKV         map[string]string         //only used for Newconfig commands
	Config        shardctrler.Config        //only used for Newconfig commands
	DisableShards []int                     //only used for Disable commands
	CurrConfigNum int                       //only used for Disable commands
}

func cmp(a Op, b Op) bool {
	//only for the comparison of Get/Put/Append Ops
	if a.Type != b.Type || a.Key != b.Key || a.Value != b.Value || a.Version != b.Version || a.ID != b.ID {
		return false
	}
	return true
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

	mck *shardctrler.Clerk

	currConfigNum int                       //current config number(already got consensus)
	currShards    [shardctrler.NShards]bool //shards that this group is responsible for now(already got consensus)(maybe slightly outdated, then updated to expectedShards)
	currConfig    shardctrler.Config        //used for migration

	isLeader         int32
	isPollCtrlerLive *int32 //used to prevent two pollCtrler running at the same time.
}

type handlerReply struct { //message between applyListener and RPC Handlers.
	err   Err
	value string //for Get.
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf("server %d: process Get\n", kv.me)

	isLeader := atomic.LoadInt32(&kv.isLeader)
	if isLeader == 0 {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
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

	reply.Err = hreply.err
	reply.Value = hreply.value

	DPrintf("finish processing get: %v, Err=%v\n", reply.Value, reply.Err)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf("server %d: process PutAppend\n", kv.me)

	isLeader := atomic.LoadInt32(&kv.isLeader)
	if isLeader == 0 {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
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
		kv.mu.Unlock()
		return
	}

	newCmd := Op{Type: args.Op, Key: args.Key, Value: args.Value, Version: args.Version, ID: args.ID}
	index, _, _ := kv.rf.Start(newCmd)

	kv.pendingChannels[index] = make(chan handlerReply) //used to receive Err message
	kv.pendingMap[index] = newCmd                       //used to save newCmd, for applyListener() to query.
	ch := kv.pendingChannels[index]
	kv.mu.Unlock()

	hreply := <-ch

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

	// kill pollCtrler HERE
	atomic.StoreInt32(kv.isPollCtrlerLive, 0)

	atomic.StoreInt32(&kv.isLeader, 0)
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
					atomic.StoreInt32(&kv.isLeader, 1)
					atomic.StoreInt32(kv.isPollCtrlerLive, 0)
					kv.isPollCtrlerLive = new(int32)
					*kv.isPollCtrlerLive = 1
					go kv.pollCtrler(100*time.Millisecond, kv.isPollCtrlerLive)
				} else {
					kv.abortLeader()
				}
				kv.mu.Unlock()
				continue
			}

			if cmd.Type == "Disable" {
				if cmd.CurrConfigNum < kv.currConfigNum {
					//should return ErrUpdated
					goto sendHreply
				}
				for _, shard := range cmd.DisableShards {
					kv.currShards[shard] = false
				}
				goto sendHreply
			}

			if cmd.Type == "Newconfig" {
				kv.currConfig = cmd.Config
				kv.currConfigNum++
				kv.currShards = cmd.Shards //update currShards
				for k, v := range cmd.NewKV {
					kv.kvMap[k] = v
				}

				goto sendHreply

				//kv.mu.Unlock()
				//continue
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
				if kv.currShards[key2shard(cmd.Key)] == false {
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
				if cmp(cmd, oldCmd) == false {
					kv.abortLeader()
				} else {
					DPrintf("%d send out of loop\n", kv.me)
					var hreply handlerReply
					//check whether responsible for the key
					if cmd.Type != "Newconfig" && cmd.Type != "Disable" && kv.currShards[key2shard(cmd.Key)] == false {
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
						} else if cmd.Type == "Disable" {
							if cmd.CurrConfigNum < kv.currConfigNum {
								//should return ErrUpdated
								hreply.err = ErrUpdated
							} else {
								hreply.err = OK
							}
						} else {
							//Put or Append or Newconfig
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
				e.Encode(kv.currConfigNum)
				e.Encode(kv.currShards)
				e.Encode(kv.currConfig)
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
		//fmt.Printf("Directly returns\n")
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var dupMap map[int]int64
	var kvMap map[string]string
	var currConfigNum int
	var currShards [shardctrler.NShards]bool
	var currConfig shardctrler.Config
	if d.Decode(&dupMap) != nil ||
		d.Decode(&kvMap) != nil ||
		d.Decode(&currConfigNum) != nil ||
		d.Decode(&currShards) != nil ||
		d.Decode(&currConfig) != nil {
		fmt.Printf("server %d readPersist(kv): decode error!", kv.me)
	} else {
		kv.dupMap = dupMap
		kv.kvMap = kvMap
		kv.currConfigNum = currConfigNum
		kv.currShards = currShards
		kv.currConfig = currConfig
	}
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("server %d: process Get\n", kv.me)

	isLeader := atomic.LoadInt32(&kv.isLeader)
	if isLeader == 0 {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Migrate: Find %d leader\n", kv.me)

	//check whether we are outdated

	//fall behind
	//tell the other group to wait
	if args.CurrConfigNum > kv.currConfigNum {
		reply.Err = ErrNotPrepared
		fmt.Printf("Migrate: ErrNotPrepared\n")
		fmt.Printf("%v,%v\n", args.CurrConfigNum, kv.currConfigNum)
		return
	}
	//in same ConfigNum
	if args.CurrConfigNum == kv.currConfigNum {
		//TODO: disable some kv
		newCmd := Op{Type: "Disable", Key: "Disable_invalid", Value: "Disable_invalid", DisableShards: args.Shards, CurrConfigNum: kv.currConfigNum}
		index, _, _ := kv.rf.Start(newCmd)

		kv.pendingChannels[index] = make(chan handlerReply)
		kv.pendingMap[index] = newCmd
		ch := kv.pendingChannels[index]

		kv.mu.Unlock()

		hreply := <-ch

		if hreply.err == ErrWrongLeader {
			fmt.Printf("ErrWrongLeader in Migrate\n")
			reply.Err = ErrWrongLeader
			return
		}
		if hreply.err == ErrUpdated {
			fmt.Printf("ErrUpdated in Migrate\n")
		} else if hreply.err != OK {
			fmt.Printf("!!!!!, %v\n", hreply.err)
		}
		kv.mu.Lock()
	}
	reply.Err = OK

	var shardfilter [shardctrler.NShards]bool //all initialized to false
	for _, shard := range args.Shards {
		shardfilter[shard] = true
	}

	reply.KvMap = make(map[string]string)

	for k, v := range kv.kvMap {
		if shardfilter[key2shard(k)] {
			reply.KvMap[k] = v
		}
	}

	return
}

func (kv *ShardKV) askMissing(newshards map[int][]int, currConfig shardctrler.Config, currConfigNum int) map[string]string {
	//not holding the lock
	result := make(map[string]string)
	for gid, shards := range newshards {
		if servers, ok := currConfig.Groups[gid]; ok {
			got := false
			for !got {
				// try each server for the shard.
				for si := 0; si < len(servers); si++ {
					srv := kv.make_end(servers[si])
					args := MigrateArgs{shards, currConfigNum}
					var reply MigrateReply
					ok := true
					ok = srv.Call("ShardKV.Migrate", &args, &reply)
					for ok && (reply.Err == ErrNotPrepared) {
						time.Sleep(100 * time.Millisecond)
						reply = MigrateReply{}
						ok = srv.Call("ShardKV.Migrate", &args, &reply)
					}
					if ok && (reply.Err == OK) {
						for k, v := range reply.KvMap {
							result[k] = v
						}
						got = true
						break
					} else if ok && (reply.Err == ErrWrongLeader) {
						continue
					}
				}
			}

		} else {
			panic("gid not found in currConfig.Groups in function askMissing.")
		}
	}
	return result
}

//only leader can poll
func (kv *ShardKV) pollCtrler(duration time.Duration, islive *int32) {
	for !kv.killed() {

		//check liveness

		if atomic.LoadInt32(islive) == 0 {
			return
		}

		kv.mu.Lock()
		//query for new configuration
		newConfig := kv.mck.Query(kv.currConfigNum + 1)
		if newConfig.Num != kv.currConfigNum+1 {
			//get an old config
			kv.mu.Unlock()
			time.Sleep(duration)
			continue
		}

		//get a new config

		expectedShards := kv.currShards //array assignment, deep copy.

		//parse the config
		//newshards that need to be fetched from other groups
		newshards := make(map[int][]int) //gid->shards
		if kv.currConfigNum+1 != 1 {
			//not needed if this is the first valid config(i.e. currConfigNum+1==1)
			for shard, gid := range newConfig.Shards {
				if gid == kv.gid && expectedShards[shard] == false {
					originalGid := kv.currConfig.Shards[shard]
					_, ok := newshards[originalGid]
					if !ok {
						newshards[originalGid] = make([]int, 0)
					}
					newshards[originalGid] = append(newshards[originalGid], shard)
				}
			}
		}
		//update expectedShards
		for shard, gid := range newConfig.Shards {
			if gid == kv.gid {
				expectedShards[shard] = true
				//fmt.Printf("gid %d is responsible for shard %d\n", gid, shard)
			} else {
				expectedShards[shard] = false
			}
		}

		//ask for missing shards synchronously
		var newKV map[string]string
		if kv.currConfigNum+1 != 1 {
			tmpcurrConfig := kv.currConfig
			tmpcurrConfigNum := kv.currConfigNum
			kv.mu.Unlock()
			//not needed if this is the first valid config(i.e. currConfigNum+1==1)
			newKV = kv.askMissing(newshards, tmpcurrConfig, tmpcurrConfigNum)
			kv.mu.Lock()
		}

		//acceptable to continue to store shards that it no longer owns
		//a possible optimization is to discard shards when all groups have advanced above those shards' currConfigNum
		//but it is built on this, and we are not required to do so.

		//feed new config to raft
		newCmd := Op{Type: "Newconfig", Key: "Newconfig_invalid", Value: "Newconfig_invalid", Shards: expectedShards, NewKV: newKV, Config: newConfig}
		index, _, _ := kv.rf.Start(newCmd)

		kv.pendingChannels[index] = make(chan handlerReply)
		kv.pendingMap[index] = newCmd //used to save newCmd, for applyListener() to query.
		ch := kv.pendingChannels[index]
		kv.mu.Unlock()

		hreply := <-ch

		if hreply.err == ErrWrongLeader {
			fmt.Printf("ErrWrongLeader in pollctrler\n")
			return
		}
		if hreply.err != OK {
			panic("hreply.err != OK in pollctrler")
		}

		//now kv.currShards has been updated to expectedShards.

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

	kv.currConfigNum = 0

	kv.readPersist(persister.ReadSnapshot())

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.isLeader = 0
	kv.isPollCtrlerLive = new(int32)
	*kv.isPollCtrlerLive = 0

	go kv.applyListener()

	return kv
}
