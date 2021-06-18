package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"

	"runtime"
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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap           map[string]string
	pendingChannels map[int]chan handlerReply //map from log index to channel. only valid when it is leader
	pendingMap      map[int]Op                //map from log index to cmd
	dupMap          map[int]int64             //map from client to version
}

type handlerReply struct { //message between applyListener and RPC Handlers.
	err   Err
	value string //for Get.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
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

	newCmd := Op{"Get", args.Key, "invalid", -1, -1, -1}
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

	DPrintf("finish processing putappend: %v, Err=%v\n", newCmd.Value, hreply.err)

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyListener() {
	for !kv.killed() {
		DPrintf("%d listening\n", kv.me)
		DPrintf("current goroutine number:%d", runtime.NumGoroutine())
		msg := <-kv.applyCh
		DPrintf("%d get reply\n", kv.me)

		if msg.CommandValid {
			kv.mu.Lock()
			DPrintf("%d get lock in applyListener\n", kv.me)
			cmd := msg.Command.(Op)
			DPrintf("Commad content: %s,%s,%s\n", cmd.Type, cmd.Key, cmd.Value)

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
				//apply changes
				kv.dupMap[cmd.ID] = cmd.Version
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
					if cmd.Type == "Get" {
						elem, ok := kv.kvMap[cmd.Key]
						if ok {
							hreply.err = OK
							hreply.value = elem
						} else {
							hreply.err = ErrNoKey
						}
					} else {
						hreply.err = OK
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
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			//TODO
			//snapshot
		} else {
			DPrintf("Newleader msg!\n")
			kv.mu.Lock()
			newCmd := Op{"Newleader", "invalid", "invalid", -1, -1, kv.me} //dummy cmd
			kv.rf.Start(newCmd)
			kv.mu.Unlock()
		}

	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.pendingChannels = make(map[int]chan handlerReply)
	kv.pendingMap = make(map[int]Op)
	kv.dupMap = make(map[int]int64)

	go kv.applyListener()

	return kv
}
