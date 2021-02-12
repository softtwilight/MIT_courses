package mr

import (
	"fmt"
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
	S *JobState
	T *TaskPool
	W *sync.Map
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

func (m *Master) ReportResult(args *ResultReq, reply *ResultRes) error {

	if len(args.M) == 0 {
		reply.code = 1
		reply.Msg = "The report cannot be empty"
		return nil
	}

	if ws, ok := m.W.Load(args.WorkerID); ok {
		w := ws.(*WokerSession)
		switch args.Code {
		case 0:
			if w.T == nil {
				reply.Msg = "shut down!"
				reply.Code = 1
				return nil
			}
			dispatcher.ReduceSourceChan <- &ReduceSource{
				MIdx:      w.T.Conf.MNum,
				MapSource: args.M,
			}
		case 1:
			if w.T == nil {
				reply.Msg = "shut down!"
				reply.Code = 1
				return nil
			}
			m.S.MatrixSource[m.S.MC][w.T.Conf.RNum] = "done"
		case 2:
			task := w.T
			m.W.Delete(args.WorkerId)
			task.Status = 0
			m.T.Pool <- task
			reply.Code = 0
			return nil
		default:
			reply.Code = 1
			reply.Msg = fmt.Sprintf("Unkown code: %d", args.code)
			return nil
		}
		w.Mux.Lock()
		defer w.Mux.Unlock()
		w.Status = 0
		w.T = nil
		w.LastPingTs = time.Now().UnixNano() / 1e6
		reply.code = 0
		return nil
	}
	reply.Code = 1
	reply.Msg = "unregistered"
	return nil

}

func (d *Dispatcher) cleanSession() {
	for workId := range d.CleanWorkerChan {
		if w, ok := d.M.W.Load(workId); ok {
			worker := w.(*WokerSession)
			worker.Mux.Lock()
			task := worker.T
			worker.T = nil
			worker.Mux.Unlock()
			if task != nil {
				task.Status = 0
				d.M.T.Pool <- task
			}
			d.M.W.Delete(worker)
		}
	}
}

func (d *Dispatcher) updateJobState() {

	for rs := range d.ReduceSourceChan {
		d.M.S.MatrixSource[rs.MIdx] = rs.MapSource
		atomic.AddInt32(&d.M.S.MCDone, 1)
		if atomic.LoadInt32(&d.M.S.MCDone) == int32(d.M.S.MC) {
			for j := 0; j < d.M.S.RC; j++ {
				sources := make([]string, 0)
				for i := 0; i < d.M.S.MC; i++ {
					sources = append(sources, d.M.S.MatrixSource[i][j])
				}

				d.M.T.Pool <- &Task{
					Status: 0,
					Type:   1, // for reduce
					Conf: &TaskConf{
						Source: sources,
						RNum:   j,
						MNum:   -1,
						RC:     d.M.S.RC,
					},
				}
				d.M.S.MatrixSource[d.M.S.MC][j] = "created"
			}
		}
	}
}

func (d *Dispatcher) run() {
	go d.cleanSession()
	go d.updateJobState()
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
	res := false
	count := 0
	for _, v := range m.S.MatrixSource[m.S.MC] {
		if v == "done" {
			count++
		}
	}
	if count == m.S.RC {
		if len(m.T.Pool) != 0 {
			return false
		}
		if m.S.allDone == 0 {
			close(m.T.Pool)
			m.S.allDone = 1
		}
		c := 0
		m.W.Range(func(key, value interface{}) bool {
			w := value.(*WokerSession)
			if w.T != nil {
				c++
			}
			return true
		})
		if c == 0 {
			res = true
		}
	}

	return res
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	sources := make([][]string, len(files)+1)
	for i := 0; i < len(sources); i++ {
		sources[i] = make([]string, nReduce)
	}
	m.S = &JobState{
		MatrixSource: sources,
		MC:           len(files),
		RC:           nReduce,
		nextWorkerID: uint64(0),
	}

	m.T = &TaskPool{Pool: make(chan *Task, len(files))}
	m.W = &sync.Map{}
	dispatcher = &Dispatcher{
		TimeOut:          10 * time.Second,
		M:                &m,
		ReduceSourceChan: make(chan *ReduceSource, nReduce),
		CleanWorkerChan:  make(chan uint64, len(files)),
	}
	dispatcher.run()
	for i, file := range files {
		m.T.Pool <- &Task{
			Status: 0,
			Type:   0,
			Conf: &TaskConf{
				Source: []string{file},
				MNum:   i,
				RNum:   -1,
				RC:     nReduce,
			},
		}
	}
	m.server()
	return &m
}
