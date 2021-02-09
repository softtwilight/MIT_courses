package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

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

var workerID uint64

const (
	CallRegister = "Master.RegisterWorker"
	CallPingPong = "Master.PingPong"
	CallGetTask  = "Master.GetTaskWorker"
	CallReport   = "Master.ReportResult"
)

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	workerID = Register()
	go func() {
		tc := time.NewTicker(10 * time.Second)
		defer tc.Stop()
		for {
			<-tc.C
			// send heart bit
			PingPong()
		}
	}()

	var task *Task
	for {
		task = GetTask()
		res, err := DispatchTask(mapf, reducef, task)
		if err != nil {
			continue
		}
		Report(res)
	}
}

func Register() uint64 {
	args, reply = RegisterReq{}, RegisterRes{}
	call(CallRegister, &args, &reply)
	return reply.WorkerID
}

func PingPong() {
	args, reply := Ping{WorkerID: workerID}, Pong{}
	call(CallPingPong, &args, &reply)
}

func GetTask() *Task {
	args, reply := GetTaskReq{WorkerID: workerID}, GetTaskRes{}
	call(CallGetTask, &args, &reply)
	return reply.T
}

func Report(res *ResultReq) {
	reply := &ResultRes{}
	call(CallReport, res, reply)
}

func DispatchTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, task *Task) (*ResultReq, error) {

	if task == nil {
		return nil, fmt.Errorf("retry")
	}

	var res []string
	req := &ResultReq{WorkerId: workerID}

	if task.Type == 0 {
		res, _ = doMap(mapf, task)
		req.Code = 0
	} else if task.Type == 1 {
		res, _ = doReduce(reducef, task)
		req.Code = 1
	} else if task.Type == 2 {
		os.Exit(0)
	}
	if len(res) == 0 {
		return nil, fmt.Errorf("retry")
	}
	req.M = res
	return req, nil

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
