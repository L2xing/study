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
	addr string
}

type RegisterReply struct {
	success bool
}

// 2. Map Request
type MapReqArgs struct {
	fileName string
}

type MapReqReply struct {
	success bool
}

// 3. Map Done
type MapDoneArgs struct {
	fileName string
	shuffles map[string]string
}

type MapDoneReply struct {
	success bool
}

// 4. Reduce Request
type ReduceReqArgs struct {
	shuffleName string
	shuffles    []string
}

type ReduceReqReply struct {
	success bool
}

// 5. Map Done
type ReduceDoneArgs struct {
	shuffleName string
	result      string
}

type ReduceDoneReply struct {
	success bool
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
