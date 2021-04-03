package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu         sync.Mutex
	lastServer int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	args := GetArgs{key}
	reply := GetReply{}

	// You will have to modify this function.
	for i := 0; i < len(ck.servers); i++ {
		ok := false
		for !ok {
			ok = ck.servers[(i+ck.lastServer)%len(ck.servers)].Call("KVServer.Get", &args, &reply)
		}
		switch reply.Err {
		case OK:
			return reply.Value
		case ErrNoKey:
			return ""
		case ErrWrongLeader:
			continue
		}
	}
	panic("clerk get: traversed all servers but no one available.")
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{key, value, op}
	reply := PutAppendReply{}

	for i := 0; i < len(ck.servers); i++ {
		ok := false
		for !ok {
			ok = ck.servers[(i+ck.lastServer)%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)
		}
		switch reply.Err {
		case OK:
			return
		case ErrNoKey:
			fmt.Printf("PutAppend: no key\n")
			return
		case ErrWrongLeader:
			continue
		}
	}
	panic("clerk get: traversed all servers but no one available.")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
