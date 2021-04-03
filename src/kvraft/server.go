package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

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
	Type  string
	Key   string
	Value string
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
	pendingChannels map[int]chan raft.ApplyMsg //only valid when it is leader
	version         int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	elem, ok := kv.kvMap[args.Key]
	if ok {
		reply.Err = OK
		reply.Value = elem
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newCmd := Op{args.Op, args.Key, args.Value}
	index, _, isLeader := kv.rf.Start(newCmd)
	if !isLeader {
		DPrintf("%d Not leader\n", kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	elem := kv.kvMap[args.Key]
	kv.pendingChannels[index] = make(chan raft.ApplyMsg)
	ch := kv.pendingChannels[index]
	kv.mu.Unlock()
	DPrintf("Find %d leader\n", kv.me)
	msg := <-ch
	kv.mu.Lock()
	cmd := msg.Command.(Op)

	if cmd != newCmd {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}

	//update regardless of leadership.
	switch cmd.Type {
	case "Append":
		kv.kvMap[cmd.Key] = elem + cmd.Value
	case "Put":
		kv.kvMap[cmd.Key] = cmd.Value
	default:
		panic(fmt.Sprintf("Putappend: wrong cmd.Type: %s!\n", cmd.Type))
	}
	DPrintf("finish processing putappend\n")

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
		DPrintf("listening\n")
		msg := <-kv.applyCh

		DPrintf("get reply\n")
		if msg.CommandValid {
			kv.mu.Lock()
			ch, ok := kv.pendingChannels[msg.CommandIndex]
			if ok {
				//some handlers are pending
				ch <- msg
			} else {
				//no handler pending.
				cmd := msg.Command.(Op)
				elem := kv.kvMap[cmd.Key]
				switch cmd.Type {
				case "Append":
					kv.kvMap[cmd.Key] = elem + cmd.Value
				case "Put":
					kv.kvMap[cmd.Key] = cmd.Value
				default:
					panic(fmt.Sprintf("applyListener: wrong cmd.Type: %s!\n", cmd.Type))
				}
			}
			kv.mu.Unlock()
		} else {
			//TODO
			//snapshot
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
	kv.pendingChannels = make(map[int]chan raft.ApplyMsg)

	go kv.applyListener()

	return kv
}
