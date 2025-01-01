package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	// "os"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.

	// mapper
	mapperLock    sync.Mutex
	mappers       map[string]bool
	mapperShuffle map[string][]string

	// ReducerKey
	reducers map[string]bool

	// all
	mutexWork       sync.Mutex
	idelWorks       []WorkerInfo
	processingWorks []WorkerInfo
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.mutexWork.Lock()
	defer c.mutexWork.Unlock()
	workerAddr := args.Addr

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
		reply.Success = true
		return nil
	}

	c.idelWorks = append(c.idelWorks, WorkerInfo{Addr: args.Addr, State: 0})
	reply.Success = true
	return nil
}

func (c *Coordinator) MapDone(args *MapDoneArgs, reply *MapDoneReply) error {
	c.mapperLock.Lock()
	defer c.mapperLock.Unlock()
	// 1. 如果 fileName已经处理过直接跳过
	done, ok := c.mappers[args.FileName]
	if ok && done {
		reply.Success = true
		return nil
	}

	// 2. 合并fileName的shuffle
	c.mappers[args.FileName] = true
	for k, v := range args.Shuffles {
		shuffles, ok := c.mapperShuffle[k]
		if !ok {
			shuffles = make([]string, 1)
			c.mapperShuffle[k] = shuffles
		}
		c.mapperShuffle[k] = append(shuffles, v...)
		c.mappers[k] = true
	}

	// 3. 释放一个worker
	c.releaseWorker(args.Addr)
	return nil
}

func (c *Coordinator) releaseWorker(addr string) {
	c.mutexWork.Lock()
	defer c.mutexWork.Unlock()

	// 1. 释放一个worker
	delIdx := -1
	var workerInfo WorkerInfo
	for idx, worker := range c.processingWorks {
		if strings.Compare(worker.Addr, addr) == 0 {
			delIdx = idx
			workerInfo = worker
			break
		}
	}
	if delIdx == -1 {
		return
	}

	// 2. 恢复一个worker
	c.processingWorks = append(c.processingWorks[:delIdx], c.processingWorks[delIdx+1:]...)
	c.idelWorks = append(c.idelWorks, workerInfo)
}

func (c *Coordinator) applyWorker() string {
	c.mutexWork.Lock()
	defer c.mutexWork.Unlock()
	fmt.Println("申请worker...")
	if len(c.idelWorks) == 0 {
		fmt.Println("当前没有空闲worker...")
		return ""
	}
	idx := 0
	workerInfo := c.idelWorks[idx]

	c.processingWorks = append(c.processingWorks, workerInfo)
	c.idelWorks = c.idelWorks[idx+1:]

	fmt.Println("申请到worker... addr:", workerInfo.Addr)
	return workerInfo.Addr
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
	c := Coordinator{}
	c.mappers = make(map[string]bool)
	c.mapperShuffle = make(map[string][]string)

	c.reducers = make(map[string]bool)
	c.idelWorks = make([]WorkerInfo, 0)
	c.processingWorks = make([]WorkerInfo, 0)

	// Your code here.
	// 1. 定期检测
	go func() {
		for {
			idelCnt := 0
			for _, worker := range c.idelWorks {
				fmt.Printf("worker addr:%s\n", worker.Addr)
				idelCnt++
			}
			processingCnt := 0
			for _, worker := range c.processingWorks {
				fmt.Printf("worker addr:%s\n", worker.Addr)
				processingCnt++
			}
			totalCnt := idelCnt + processingCnt
			fmt.Printf("worker count:%d, idelcnt:%d, processingCnt:%d\n", totalCnt, idelCnt, processingCnt)
			time.Sleep(1 * time.Second)
		}
	}()

	// 2. Task分配
	for _, fileName := range files {
		c.mappers[fileName] = false
	}

	go func() {
		fmt.Println("task begin")

		// 1. map分配
		for {
			boolAllDone := true
			for fileName, done := range c.mappers {
				if done {
					continue
				}
				boolAllDone = false
				// 1. 获取一个worker
				workerAddr := c.applyWorker()
				if strings.Compare(workerAddr, "") == 0 {
					time.Sleep(1 * time.Second)
					continue
				}

				// 2. 调用worker的MapReq
				go func() {
					success := CallMapReq(workerAddr, fileName)
					if !success {
						c.releaseWorker(workerAddr)
						fmt.Println("call fail release addr:", workerAddr)
					}
				}()
			}
			if boolAllDone {
				break
			}
			fmt.Println("in map")
			time.Sleep(1 * time.Second)
		}

		// 2. reduce分配
		for {
			allDone := true
			for k, done := range c.reducers {
				shuffles, ok := c.mapperShuffle[k]
				if !ok || len(shuffles) == 0 {
					c.reducers[k] = true
					continue
				}
				if done {
					continue
				}
				allDone = false

				// 1. 获取一个worker
				workerAddr := c.applyWorker()
				if strings.Compare(workerAddr, "") == 0 {
					time.Sleep(1 * time.Second)
					continue
				}

				// 2. 调用worker的ReduceReq
				go func() {
					success := CallReduceReq(workerAddr, k, shuffles)
					if !success {
						c.releaseWorker(workerAddr)
						fmt.Println("call fail release addr:", workerAddr)
					}
				}()
				time.Sleep(1 * time.Second)
			}
			if allDone {
				break
			}
			fmt.Println("in reduce")
			time.Sleep(1 * time.Second)
		}

		fmt.Println("task done")
	}()

	c.server()
	return &c
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.

func CallMapReq(workerAddr, fileName string) bool {
	args := MapReqArgs{FileName: fileName}
	reply := MapReqReply{}
	fmt.Println("Map任务调用 addr:" + workerAddr + " ReducerKey:" + fileName + " ReducerKey:" + fileName)
	ok := callWorker(workerAddr, "WorkerInfo.MapReq", &args, &reply)
	if !ok {
		return false
	}
	return reply.Success
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
		log.Fatalf("call worker error:%v", anyError)
	}()

	c, err := rpc.DialHTTP("tcp", workerAddr)
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	err = c.Call(rpcName, args, reply)

	closeErr := c.Close()
	if closeErr != nil {
		log.Fatal("close error:", closeErr)
		return false
	}

	if err == nil {
		return true
	}

	log.Fatalf("call rpc error:%v", err.Error())
	return false
}
