package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var dispatcher *Dispatcher

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

func (m *Master) RegisterWorker(args *RegisterReg, reply *RegisterRes) error {
	_ = args
	for {
		assignID := atomic.LoadUint64(&m.S.nextWorkerID)
		if atomic.CompareAndSwapUint64(&m.S.nextWorkerID, assignID, assignID+1) {
			reply.WorkerID = assignID
			ws := &WokerSession{
				WorkerID:     assignID,
				Status:       0, // 0 for good, 1 for lost
				T:            nil,
				LastPingTs:   time.Now().UnixNano() / 1e6,
				Mux:          &sync.RWMutex{},
				PingPongChan: make(chan struct{}),
			}
			m.W.Store(assignID, ws)
			go ws.PingPong(dispatcher.TimeOut)
			return nil
		}
		time.Sleep(10 * time.Millisecond)

	}
}

func (m *Master) GetTaskWorker(args *GetTaskReq, reply *GetTaskRes) error {
	c := time.After(5 * time.Second)
	if worker, ok := m.W.Load(args.WorkerID); ok {
		w := worker.(*WokerSession)
		select {
		case task, ok := <-m.TP.Pool:
			if !ok {
				shutdown(reply)
				m.W.Delete(w.WorkerId)
				return nil
			}
			task.Status = 1
			reply.T = task
			w.Mux.Lock()
			defer w.Mux.Unlock()
			w.Status = 1
			w.T = task

		case <-c:
		}
	} else {
		shutdown(reply)
		m.W.Delete(args.WorkerId)
	}
	return nil
}

func (m *Master) PingPong(args *Ping, reply *Pong) error {
	if ws, ok := m.W.Load(args.WorkerID); ok {
		w := ws.(*WokerSession)
		w.Mux.Lock()
		defer w.Mux.Unlock()
		w.LastPingTs = time.Now().UnixNano() / 1e6
		w.PingPongChan <- struct{}{}
	}
	reply.Code = 0
	return nil
}

func shutdown(reply *GetTaskRes) {
	reply.Msg = "shut down."
	reply.T = &Task{
		Status: 0,
		Type:   2,
		Conf: &TaskConf{
			Source: []string{},
		},
	}
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
