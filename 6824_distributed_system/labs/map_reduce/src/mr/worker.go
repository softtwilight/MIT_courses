package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
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
	CallRegister   = "Master.RegisterWorker"
	CallPingPong   = "Master.PingPong"
	CallGetTask    = "Master.GetTaskWorker"
	CallReport     = "Master.ReportResult"
	MapOutfileName = "mr-worker-%d-%d.out"
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

func doMap(mapf func(string, string) []KeyValue, task *Task) ([]string, error) {
	res := make([]string, 0)
	fileName := task.Conf.Source[0]
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		return nil, fmt.Errorf("MapTask Open file err : %s", err.Error())
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("readAll error")
	}

	cacheMap := make(map[string][]KeyValue, 0)
	for i := 0; i < task.Conf.RC; i++ {
		key := fmt.Sprintf(MapOutfileName, task.Conf.MNum, i)
		cacheMap[key] = []KeyValue{}
		res = append(res, key)
	}

	kva := mapf(fileName, string(content))
	for i := 0; i < len(kva); i++ {
		idx := ihash(kva[i].Key) % task.Conf.RC
		key := fmt.Sprintf(MapOutfileName, task.Conf.MNu, idx)
		cacheMap[key] = append(cacheMap[key], kva[i])
	}

	for key, value := range cacheMap {
		writeInterFile(key, value)
	}
	return res, nil
}

func doReduce(reducef func(string, []string) string, task *Task) ([]string, error) {
	kvs := readFiles(task)
	tmpFileName := fmt.Sprintf("mr-out-%d.%d.swap", time.Now().Unix(), task.Conf.RNum)
	outFile, _ := os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	defer outFile.Close()
	if len(kvs) == 0 {
		newName := fmt.Sprintf("mr-out-%d", task.Conf.RNum)
		_ = os.Rename(tmpFileName, newName)
		return []string{newName}, nil
	}

	sort.Sort(ByKey(kvs))
	buf := []KeyValue{kvs[0]}

	for i := 1; i < len(kvs); i++ {
		if buf[len(buf)-1].Key == kvs[i].Key {
			buf = append(buf, kvs[i])
		} else {
			out := reducef(buf[len(buf)-1].Key, toValues(buf))
			_, _ = fmt.Fprintf(outFile, "%v %v\n", buf[len(buf)-1].Key, out)
			buf = []KeyValue{kvs[i]}
		}
	}

	out := reducef(buf[len(buf)-1].Key, toValues(buf))
	_, _ = fmt.Fprintf(outFile, "%v %v\n", buf[len(buf)-1].Key, out)
	key := fmt.Sprintf("mr-out-%d", task.Conf.RNum)
	_ = os.Rename(tmpFileName, key)
	return []string{key}, nil
}

func toValues(kvs []KeyValue) []string {
	res := make([]string, 0)
	for _, kv := range kvs {
		res = append(res, kv.Value)
	}
	return res
}

func writeInterFile(fileName string, value []KeyValue) {
	sort.Sort(ByKey(value))
	outFile, _ := os.Create(fileName)
	defer outFile.Close()
	for i := 0; i < len(value); i++ {
		_, _ = fmt.Fprintf(outFile, "%v %v\n", value[i].Key, value[i].Value)
	}
}

func readFiles(task *Task) []KeyValue {

	res := make([]KeyValue, 0)
	for _, v := range task.Conf.Source {
		file, _ := os.Open(v)
		br := bufio.NewReader(file)
		for {
			line, _, c := br.ReadLine()
			if c == io.EOF {
				break
			}
			data := strings.Split(string(line), " ")
			res = append(res, KeyValue{
				Key:   data[0],
				Value: data[1],
			})
		}
		_ = file.Close()
	}
	return res
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
