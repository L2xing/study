package mr

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mappers  map[string]string
	reducers map[string]string

	// all
	mutexWork       sync.Mutex
	idelWorks       []WorkerInfo
	processingWorks []WorkerInfo
}

type WorkerInfo struct {
	Addr  string
	State int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.mutexWork.Lock()
	defer c.mutexWork.Unlock()

	workerAddr := args.addr

	hasWorker := false
	for _, worker := range c.idelWorks {
		compare := strings.Compare(workerAddr, worker.Addr)
		if compare == 0 {
			hasWorker = true
			break
		}
	}
	for _, worker := range c.processingWorks {
		compare := strings.Compare(workerAddr, worker.Addr)
		if compare == 0 {
			hasWorker = true
			break
		}
	}

	if hasWorker {
		return nil
	}

	c.idelWorks = append(c.idelWorks, WorkerInfo{Addr: args.addr, State: 0})
	return nil
}

// an example RPC handler.

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mappers = make(map[string]string)
	c.reducers = make(map[string]string)
	c.idelWorks = make([]WorkerInfo, 0)
	c.processingWorks = make([]WorkerInfo, 0)

	// Your code here.
	go func() {
		for {
			for _, worker := range c.idelWorks {
				fmt.Printf("worker addr:%s\n", worker.Addr)
			}
			for _, worker := range c.processingWorks {
				fmt.Printf("worker addr:%s\n", worker.Addr)
			}
			fmt.Printf("worker count:%d\n", len(c.idelWorks)+len(c.processingWorks))
			time.Sleep(1 * time.Second)
		}
	}()

	c.server()
	return &c
}
