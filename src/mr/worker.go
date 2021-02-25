package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//wrapper function of mapf
//called in Worker()
func mapfwrapper(mapf func(string, string) []KeyValue, filename string, nReduce int) {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	//partitioning the intermediate key space into nReduce pieces
	intermediate := make([][]KeyValue, nReduce)
	for i := range kva {
		index := ihash(kva[i].Key) % nReduce
		intermediate[index] = append(intermediate[index], kva[i])
	}

	//write to file
	for i := 0; i < nReduce; i++ {
		ifilename := fmt.Sprintf("mr-%s-%d", filepath.Base(filename), i)
		ifile, err := os.Create(ifilename)
		if err != nil {
			log.Fatalf("cannot open %v", ifilename)
		}
		enc := json.NewEncoder(ifile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write %v", ifilename)
			}
			//fmt.Fprintf(ofile, "%v %v\n", intermediate[i][j].Key, intermediate[i][j].Value)
		}
		ifile.Close()

	}

	//report finish is handled in Worker() by the loop

	return
}

//wrapper function of reducef
//called in Worker()
func reducefwrapper(reducef func(string, []string) string, rnumber int, nMap int) {
	//read from intermediate files
	intermediate := []KeyValue{}
	fileread := 0
	for fileread < nMap {
		reply := AskFile()
		if reply.Status == 1 { //exception. may because we were running too slow and the coordinator had deemed us dead
			//for output simplicity, the following Printf sentence is commented out.
			//fmt.Printf("worker %d met exception.", os.Getpid())
			return //discard current work and ask work again
		}
		fileread += len(reply.Filenames)

		//read files got from the reply
		for _, filename := range reply.Filenames {
			//fmt.Println(filename)
			ifile, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			dec := json.NewDecoder(ifile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil { //eof
					break
				}
				intermediate = append(intermediate, kv)
			}
			ifile.Close()
		}

		//sleep for a while
		if fileread != nMap {
			time.Sleep(100 * time.Millisecond)
		}

	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", rnumber)
	ofile, _ := ioutil.TempFile(".", oname)
	onametmp := ofile.Name()

	//copied from mrsequencial.go
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	if err := os.Rename(onametmp, oname); err != nil {
		log.Fatalf("cannot rename %s", onametmp)
	}

	//report finish is handled in Worker() by the loop

	return
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply := AskWork()

		//do work
		switch reply.Wtype {
		case 0:
			mapfwrapper(mapf, reply.Filename, reply.NReduce)
		case 1:
			reducefwrapper(reducef, reply.Rnumber, reply.NMap)
		case 2:
			return //exit
		case 3:
			time.Sleep(time.Second) //wait for a while
		}

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//function to ask the coordinator for work
//also used when a worker finishes
//repeat 10 times when timeout
func AskWork() WorkRequestReply {
	args := RequestArgs{os.Getpid(), 0}
	reply := WorkRequestReply{}
	repeat := 0

	ok := call("Coordinator.WorkHandler", &args, &reply)
	for ok != true && repeat < 10 { //get timeout
		ok = call("Coordinator.WorkHandler", &args, &reply)
		repeat++
	}
	if repeat == 10 { //repeat 10 times
		log.Fatal("AskWork: max retries exceeded.")
	}
	return reply
}

//function to ask the coordinator for intermediate file names
//repeat 10 times when timeout
func AskFile() FileRequestReply {
	args := RequestArgs{os.Getpid(), 1}
	reply := FileRequestReply{}
	repeat := 0

	ok := call("Coordinator.FileHandler", &args, &reply)
	for ok != true && repeat < 10 { //get timeout
		ok = call("Coordinator.FileHandler", &args, &reply)
		repeat++
	}
	if repeat == 10 { //repeat 10 times
		log.Fatal("AskFile: max retries exceeded.")
	}
	return reply
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
