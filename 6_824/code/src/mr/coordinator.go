package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int

	// mapper
	mappers map[string]bool

	// ReducerKey

	// all

}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	return nil
}

func (c *Coordinator) MapDone(args *MapDoneArgs, reply *MapDoneReply) error {
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
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
// nReduce is the number of ReducerKey tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Printf("files:%v", files)
	c := InitCoordinator(files, nReduce)
	c.server()
	return c
}

func InitCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nReduce = nReduce

	return &c
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.

func CallMapReq(workerAddr, fileName string) bool {
	args := MapReqArgs{FileName: fileName}
	reply := MapReqReply{}
	fmt.Println("Map任务调用 addr:" + workerAddr + " fileName:" + fileName)
	ok := callWorker(workerAddr, "WorkerInfo.MapReq", &args, &reply)
	return ok && reply.Success
}

func CallReduceReq(workerAddr, fileName string, shuffles []string) bool {
	args := ReduceReqArgs{fileName, shuffles}
	reply := ReduceReqReply{}
	fmt.Println("addr:" + workerAddr + " ReducerKey:" + fileName + " ReducerKey:" + fileName)
	ok := callWorker(workerAddr, "WorkerInfo.ReduceReq", &args, &reply)
	if !ok {
		return false
	}
	return reply.Success
}

func callWorker(workerAddr, rpcName string, args interface{}, reply interface{}) bool {
	defer func() {
		anyError := recover()
		if anyError != nil {
			log.Printf("call worker error:%v", anyError)
		}
	}()

	c, err := rpc.DialHTTP("tcp", workerAddr)
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Printf("dialing:", err)
		return false
	}

	err = c.Call(rpcName, args, reply)

	closeErr := c.Close()
	if closeErr != nil {
		log.Printf("worker: %s close error:", workerAddr, closeErr)
		return false
	}

	if err == nil {
		return true
	}

	log.Printf("call rpc error:%v", err.Error())
	return false
}
