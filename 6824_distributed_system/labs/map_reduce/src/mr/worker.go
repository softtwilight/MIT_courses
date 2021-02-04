package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	reply := CallExample()
	filename := reply.fileName
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
	sort.Sort(ByKey(kva))

	enc := json.NewEncoder(file)
	for _, kv := range kva {
		err := enc.Encode(&kv)
		reduceTaskNum := ihash(kv.Key) % reply.nReduce
		oname := fmt.Sprintf("mr-%d-%d", reply.mapTaskNum, reduceTaskNum)
		ofile, _ := os.Create(oname)
		fmt.Fprintf(ofile, "%v\n", err)
	}


}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() *ExampleReply{

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 0

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.fileName %v\n", reply.fileName)
	return &reply
}

func CallAskAJob() *JobReply{

	// declare an argument structure.
	args := JobArgs{}


	// declare a reply structure.
	reply := JobReply{}

	// send the RPC request, wait for the reply.
	call("Master.JobDispatch", &args, &reply)

	// reply.Y should be 100.
	//fmt.Printf("reply.fileName %v\n", reply.fileName)
	return &reply
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
