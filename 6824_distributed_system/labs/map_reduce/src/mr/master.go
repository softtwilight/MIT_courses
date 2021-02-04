package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	S  *JobState
	TP *TaskPool
	W  *sync.Map
}

type JobState struct {
	MatrixSource [][]string
	MC           int
	RC           int
	MCDone       int32
	nextWorkerID uint64
	allDone      int
}
type TaskPool struct {
	Pool chan *Task
}
type Task struct {
	Status int
	Type   int
	Conf   *TaskConf
}

type TaskConf struct {
	Source []string
	RNum   int
	MNum   int
	RC     int
}

type Dispatcher struct {
	TimeOut          time.Duration
	M                *Master
	ReduceSourceChan chan *ReduceSource
	CleanWorkerChan  chan uint64
}

type WokerSession struct {
	WorkerID     uint64
	Status       int
	T            *Task
	Mux          *sync.RWMutex
	LastPingTs   int64
	PingPongChan chan struct{}
}

type ReduceSource struct {
	MIdx      int
	MapSource []string
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {

	return nil
}

func (m *Master) JobDispatch(args *JobArgs, reply *JobReply) error {

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.nReduce == m.completedTask
	// Your code here.
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:         files,
		nReduce:       nReduce,
		completedTask: 0,
	}
	m.status = make([]int, len(files))

	// Your code here.

	m.server()
	return &m
}
