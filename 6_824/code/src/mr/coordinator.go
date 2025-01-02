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
	mappers       map[string]int
	mapperShuffle map[string][]string

	// ReducerKey
	reducers map[string]int

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
	log.Printf("MapDone started, addr:%s, fileName:%s, shuffles:%v", args.Addr, args.FileName, len(args.Shuffles))
	// 1. 如果 fileName已经处理过直接跳过
	state, ok := c.mappers[args.FileName]
	if ok && state == 2 {
		log.Printf("MapDone done, addr:%s, fileName:%s \n", args.Addr, args.FileName)
		reply.Success = true
		c.releaseWorker(args.Addr)
		return nil
	}

	// 2. 合并fileName的shuffle
	c.mappers[args.FileName] = 2
	for k, v := range args.Shuffles {
		shuffles, ok := c.mapperShuffle[k]
		if !ok {
			shuffles = make([]string, 1)
			c.mapperShuffle[k] = shuffles
		}
		c.mapperShuffle[k] = append(shuffles, v...)
		c.reducers[k] = 0
	}

	// 3. 释放一个worker
	log.Printf("MapDone finished, addr:%s, fileName:%s \n", args.Addr, args.FileName)
	reply.Success = true
	c.releaseWorker(args.Addr)
	return nil
}

func (c *Coordinator) ReduceDone(args *ReduceDoneArgs, reply *MapDoneReply) error {
	c.mapperLock.Lock()
	defer c.mapperLock.Unlock()
	log.Printf("Reducer任务完成。reduceKey: %s, output: %s\n", args.ShuffleName, args.Result)
	c.releaseWorker(args.Addr)
	reply.Success = true
	c.reducers[args.ShuffleName] = 2
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
	log.Printf("files:%v", files)
	c := Coordinator{}
	c.mappers = make(map[string]int)
	c.mapperShuffle = make(map[string][]string)

	c.reducers = make(map[string]int)
	c.idelWorks = make([]WorkerInfo, 0)
	c.processingWorks = make([]WorkerInfo, 0)

	// Your code here.
	// 1. 定期检测
	go func() {
		for {
			idelCnt := 0
			for _ = range c.idelWorks {
				idelCnt++
			}
			processingCnt := 0
			for _ = range c.processingWorks {
				processingCnt++
			}
			totalCnt := idelCnt + processingCnt
			log.Printf("worker count:%d, idelcnt:%d, processingCnt:%d\n", totalCnt, idelCnt, processingCnt)
			time.Sleep(1 * time.Second)
		}
	}()

	// 2. Task分配
	for _, fileName := range files {
		c.mappers[fileName] = 0
	}

	go func() {
		fmt.Println("task begin")

		// 1. map分配
		for {
			boolAllDone := true
			for fileName, state := range c.mappers {
				// 	已完成
				if state == 2 {
					continue
				}

				// 处理中
				boolAllDone = false
				if state == 1 {
					continue
				}

				// 待处理
				// 1. 获取一个worker
				workerAddr := c.applyWorker()
				if strings.Compare(workerAddr, "") == 0 {
					time.Sleep(1 * time.Second)
					continue
				}

				// 2. 调用worker的MapReq
				c.mappers[fileName] = 1
				success := CallMapReq(workerAddr, fileName)
				if !success {
					c.mappers[fileName] = 0
					fmt.Println("call fail release addr:", workerAddr)
					c.releaseWorker(workerAddr)
				}
			}
			if boolAllDone {
				break
			}
			log.Println("in map")
			time.Sleep(1 * time.Second)
		}

		// 2. reduce分配
		for {
			allDone := true
			for k, state := range c.reducers {
				shuffles, ok := c.mapperShuffle[k]
				if !ok || len(shuffles) == 0 {
					c.reducers[k] = 2
					continue
				}

				// 未处理
				if state == 2 {
					continue
				}
				allDone = false

				// 处理中
				if state == 1 {
					continue
				}

				// 未处理
				// 1. 获取一个worker
				workerAddr := c.applyWorker()
				if strings.Compare(workerAddr, "") == 0 {
					time.Sleep(1 * time.Second)
					continue
				}

				// 2. 调用worker的ReduceReq
				c.reducers[k] = 1
				success := CallReduceReq(workerAddr, k, shuffles)
				if !success {
					c.reducers[k] = 0
					c.releaseWorker(workerAddr)
					fmt.Println("call fail release addr:", workerAddr)
				}
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
	log.Println("Map任务调用 addr:" + workerAddr + " fileName:" + fileName)
	ok := callWorker(workerAddr, "WorkerInfo.MapReq", &args, &reply)
	return ok && reply.Success
}

func CallReduceReq(workerAddr, reducerKey string, shuffles []string) bool {
	args := ReduceReqArgs{reducerKey, shuffles}
	reply := ReduceReqReply{}
	log.Println("Reducer任务调用 addr:" + workerAddr + " ReducerKey:" + reducerKey)
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
