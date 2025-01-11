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
type ApplyTaskArgs struct {
}

type ApplyTaskReply struct {
	// true时下面才都有效
	Success bool

	// -1=close 0=wait 1.map 2.reduce
	Command int

	// Command=2
	MapFileName string
	NReduce     int

	// Command=3
	ReduceIdx int
	Shuffles  []string
}

// 3. Map Done
type MapDoneArgs struct {
	FileName string
	Shuffles string
}

type MapDoneReply struct {
	Success bool
}

// 4. Reduce Done
type ReduceDoneArgs struct {
	HashI      int
	OutputFile string
	Success    bool
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
