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

type RegisterReq struct {
}

type RegisterRes struct {
	WorkerID uint64
}

type GetTaskReq struct {
	WorkerID uint64
}

type GetTaskRes struct {
	Code int
	Msg  string
	T    *Task
}

type ResultReq struct {
	WorkerId uint64
	Code     int
	Msg      string
	M        []string
}

type Ping struct {
	WorkerID uint64
}

type Pong struct {
	Code int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
