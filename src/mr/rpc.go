package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//very very important:
//Must capitalize the fileds in RPC related structs!
//(for example: Status in FileRequestReply)
//otherwise it will not be set to coordinator assigned value,
//and will be regarded as private.
//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

type RequestArgs struct {
	Workerid int //use pid to represent worker
	Rtype    int //0: ask for work; 1: ask for intermediate filename(send by reduce worker)
}

type WorkRequestReply struct {
	Wtype    int    //0: map work; 1: reduce work; 2: exit work; 3: wait for others to finish
	NReduce  int    //number of reduce tasks
	NMap     int    //number of map tasks
	Filename string //input file for map work. only valid when wtype==0
	Rnumber  int    //reduce partition number. only valid when wtype==1
}

type FileRequestReply struct {
	Status    int //0: normal; 1: exception
	Filenames []string
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
