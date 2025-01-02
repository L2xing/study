package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Add your RPC definitions here.

// 1. 注册
type RegisterArgs struct {
	Addr string
}

type RegisterReply struct {
	Success bool
}

// 2. Map Request
type MapReqArgs struct {
	FileName string
	NReduce  int
}

type MapReqReply struct {
	Shuffle string
	Success bool
}

// 3. Map Done
type MapDoneArgs struct {
	Addr     string
	FileName string
	Shuffles map[string][]string
}

type MapDoneReply struct {
	Success bool
}

// 4. Reduce Request
type ReduceReqArgs struct {
	HashI    int
	Shuffles []string
}

type ReduceReqReply struct {
	HashI      int
	OutPutFile string
	Success    bool
}

// 5. Map Done
type ReduceDoneArgs struct {
	ShuffleName string
	Result      string
}

type ReduceDoneReply struct {
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
// 通过Unix Socket比使用net Socket在统一机器通信更快
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
