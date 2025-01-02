package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int

	// mapper
	mappers map[string]string

	// ReducerKey
	reducers          []bool
	reducerFilePrefix string

	// worker
	wLock  sync.Mutex
	worker []string
	curIdx int

	// ret
	retL sync.Mutex
	ret  bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.wLock.Lock()
	defer c.wLock.Unlock()

	hasWorker := false
	workerAddr := args.Addr
	for _, addr := range c.worker {
		if strings.Compare(addr, workerAddr) == 0 {
			hasWorker = true
		}
	}

	if !hasWorker {
		log.Printf("register worker: %v", workerAddr)
		c.worker = append(c.worker, workerAddr)
	}

	reply.Success = true
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
	c.retL.Lock()
	defer c.retL.Unlock()
	return c.ret
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

	// 1. 初始化mapper
	c.mappers = make(map[string]string)
	for _, file := range files {
		c.mappers[file] = ""
	}

	// 2. 初始化reducer
	c.reducers = make([]bool, nReduce)
	c.reducerFilePrefix = "mr-out"

	go c.handleMapReducer()

	return &c
}

func (c *Coordinator) handleMapReducer() {
	// 1. 提交Map任务
	for fileName, shuffle := range c.mappers {
		if len(shuffle) > 0 {
			continue
		}
		worker := c.applyWorker()
		for strings.Compare(worker, "") == 0 {
			worker = c.applyWorker()
			time.Sleep(500 * time.Millisecond)
		}
		//go func(worker, fileName string) {
		shuffle := CallMapReq(worker, fileName, c.nReduce)
		c.mappers[fileName] = shuffle
		//}(worker, fileName)
	}

	// 2. 循环直到Map全部处理完毕
	for {
		mapAllDone := true
		for _, shuffle := range c.mappers {
			// todo map阶段确实存在 shuffle == 0
			if len(shuffle) > 0 {
				continue
			}
			mapAllDone = false
			break
		}

		if mapAllDone {
			break
		}

		log.Println("in Mapping...")
		time.Sleep(1 * time.Second)
	}

	// 3. 开启reducer阶段
	for idx := range c.reducers {
		worker := c.applyWorker()
		for strings.Compare(worker, "") == 0 {
			worker = c.applyWorker()
			time.Sleep(500 * time.Millisecond)
		}
		shuffles := make([]string, 0)
		for _, shuffle := range c.mappers {
			shuffles = append(shuffles, shuffle)
		}
		CallReduceReq(worker, idx, shuffles)
		c.reducers[idx] = true
	}

	// 4. reducer验证
	for {
		allDone := true
		for _, done := range c.reducers {
			if done {
				continue
			}
			allDone = false
			break
		}
		log.Println("in Reduce...")
		if allDone {
			break
		}
		time.Sleep(1 * time.Second)
	}

	log.Printf("Done!\n")
	c.retL.Lock()
	defer c.retL.Unlock()
	c.ret = true
}

func (c *Coordinator) applyWorker() string {
	c.wLock.Lock()
	defer c.wLock.Unlock()
	if len(c.worker) == 0 {
		log.Println("no worker")
		return ""
	}

	firstAddr := c.worker[0]
	c.worker = c.worker[1:]
	c.worker = append(c.worker, firstAddr)
	log.Printf("apply worker: %v", firstAddr)
	return firstAddr
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.

func CallMapReq(workerAddr, fileName string, nReduce int) string {
	args := MapReqArgs{FileName: fileName, NReduce: nReduce}
	reply := MapReqReply{}
	log.Printf("Map调用 addr:%s, args:%v \n", workerAddr, args)
	ok := callWorker(workerAddr, "WorkerInfo.MapReq", &args, &reply)
	if ok && reply.Success {
		return reply.Shuffle
	}
	log.Fatalf("Map调用失败 addr:%s, args:%v \n", workerAddr, args)
	return ""
}

func CallReduceReq(workerAddr string, hashI int, shuffles []string) string {
	args := ReduceReqArgs{hashI, shuffles}
	reply := ReduceReqReply{}
	log.Printf("Reduce调用 addr:%s, args:%v \n", workerAddr, args)
	ok := callWorker(workerAddr, "WorkerInfo.ReduceReq", &args, &reply)
	log.Printf("Reduce调用结果 addr:%s, args:%v, reply:%v \n", workerAddr, args, reply)
	if ok && reply.Success {
		return reply.OutPutFile
	}
	log.Fatalf("Reduce调用失败 addr:%s, args:%v \n", workerAddr, args)
	return ""
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
		log.Printf("worker: %s close error:%v", workerAddr, closeErr)
		return false
	}

	if err == nil {
		return true
	}

	log.Printf("call rpc error:%v", err.Error())
	return false
}
