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
	mappers        map[string]string
	mappersLock    map[string]*sync.Mutex
	mapperCheckCnt map[string]int64

	// ReducerKey
	reducers          []bool
	reducersLock      []*sync.Mutex
	reducersCheckCnt  []int64
	reducerFilePrefix string

	// workers
	wLock   sync.Mutex
	workers []string
	curIdx  int

	// ret
	retL sync.Mutex
	ret  bool
}

// Your code here -- RPC handlers for the workers to call.
func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.wLock.Lock()
	defer c.wLock.Unlock()

	hasWorker := false
	workerAddr := args.Addr
	for _, addr := range c.workers {
		if strings.Compare(addr, workerAddr) == 0 {
			hasWorker = true
		}
	}

	if !hasWorker {
		log.Printf("register workers: %v", workerAddr)
		c.workers = append(c.workers, workerAddr)
	}

	reply.Success = true
	return nil
}

func (c *Coordinator) MapDone(args *MapDoneArgs, reply *MapDoneReply) error {
	return nil
}

// start a thread that listens for RPCs from workers.go
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
	if !c.ret {
		return false
	}

	// 1. 通知所有worker退出
	for _, addr := range c.workers {
		CallCloseWorker(addr)
	}

	return true
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
	c.mapperCheckCnt = make(map[string]int64)
	c.mappersLock = make(map[string]*sync.Mutex)
	for _, file := range files {
		c.mappers[file] = ""
		c.mapperCheckCnt[file] = 0
		c.mappersLock[file] = &sync.Mutex{}
	}

	// 2. 初始化reducer
	c.reducers = make([]bool, nReduce)
	c.reducersCheckCnt = make([]int64, nReduce)
	c.reducersLock = make([]*sync.Mutex, nReduce)
	for idx := range c.reducers {
		c.reducers[idx] = false
		c.reducersCheckCnt[idx] = 0
		c.reducersLock[idx] = &sync.Mutex{}
	}
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
		MapReq(c, fileName)
	}

	// 2. 循环直到Map全部处理完毕
	for {
		mapAllDone := true
		for fileName, shuffle := range c.mappers {
			// todo map阶段确实存在 shuffle == 0
			if len(shuffle) > 0 {
				if c.mapperCheckCnt[fileName] > 0 {
					// 重试检查减一
					c.mapperCheckCnt[fileName]--
				} else {
					// 重试
					MapReq(c, fileName)
				}
				continue
			}
			mapAllDone = false
		}

		if mapAllDone {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 3. 开启reducer阶段
	for idx := range c.reducers {
		ReduceReq(c, idx)
	}

	// 4. reducer验证
	for {
		allDone := true
		for idx, done := range c.reducers {
			if done {
				if c.reducersCheckCnt[idx] > 0 {
					// reducer重试检查减一
					c.reducersCheckCnt[idx]--
				} else {
					// 重试
					ReduceReq(c, idx)
				}
				continue
			}
			allDone = false
		}
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

func ReduceReq(c *Coordinator, idx int) {
	c.reducersLock[idx].Lock()
	defer c.reducersLock[idx].Unlock()

	// 1. 申请worker
	worker := c.applyWorker()
	for strings.Compare(worker, "") == 0 {
		worker = c.applyWorker()
		time.Sleep(500 * time.Millisecond)
	}

	// 2. 合并shuffles
	shuffles := make([]string, 0)
	for _, shuffle := range c.mappers {
		shuffles = append(shuffles, shuffle)
	}

	// 3. 初始化reducer
	c.reducersCheckCnt[idx] = 5

	// 4. 提交任务
	go func(worker string, idx int, c *Coordinator) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Recovered in ReduceReq: %v", r)
			}
		}()
		reduceDone := CallReduceReq(worker, idx, shuffles)
		if !reduceDone {
			c.reducersCheckCnt[idx] = 0
			return
		} else {
			c.reducersLock[idx].Lock()
			defer c.reducersLock[idx].Unlock()
			c.reducers[idx] = true
		}
	}(worker, idx, c)
}

func MapReq(c *Coordinator, fileName string) {
	c.mappersLock[fileName].Lock()
	defer c.mappersLock[fileName].Unlock()

	// 1. 申请worker
	worker := c.applyWorker()
	for strings.Compare(worker, "") == 0 {
		worker = c.applyWorker()
		time.Sleep(500 * time.Millisecond)
	}

	// 2. 初始化mapper
	c.mapperCheckCnt[fileName] = 5

	// 3. 提交任务
	go func(worker, fileName string, c *Coordinator) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Recovered in MapReq: %v", r)
			}
		}()
		shuffle, ok := CallMapReq(worker, fileName, c.nReduce)
		c.mappersLock[fileName].Lock()
		defer c.mappersLock[fileName].Unlock()
		if ok {
			c.mappers[fileName] = shuffle
		} else {
			c.mapperCheckCnt[fileName] = 0
		}
	}(worker, fileName, c)
}

func (c *Coordinator) applyWorker() string {
	c.wLock.Lock()
	defer c.wLock.Unlock()
	if len(c.workers) == 0 {
		log.Println("no workers")
		return ""
	}

	firstAddr := c.workers[0]
	c.workers = c.workers[1:]
	c.workers = append(c.workers, firstAddr)
	log.Printf("apply workers: %v", firstAddr)
	return firstAddr
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.

func CallMapReq(workerAddr, fileName string, nReduce int) (string, bool) {
	args := MapReqArgs{FileName: fileName, NReduce: nReduce}
	reply := MapReqReply{}
	log.Printf("Map调用 addr:%s, args:%v \n", workerAddr, args)
	ok := callWorker(workerAddr, "WorkerInfo.MapReq", &args, &reply)
	if ok && reply.Success {
		return reply.Shuffle, true
	}
	log.Printf("Map调用失败 addr:%s, args:%v \n", workerAddr, args)
	return "", false
}

func CallReduceReq(workerAddr string, hashI int, shuffles []string) bool {
	args := ReduceReqArgs{hashI, shuffles}
	reply := ReduceReqReply{}
	log.Printf("Reduce调用 addr:%s, args:%v \n", workerAddr, args)
	ok := callWorker(workerAddr, "WorkerInfo.ReduceReq", &args, &reply)
	log.Printf("Reduce调用结果 addr:%s, args:%v, reply:%v \n", workerAddr, args, reply)
	if ok && reply.Success {
		return true
	}
	log.Printf("Reduce调用失败 addr:%s, args:%v \n", workerAddr, args)
	return false
}

func CallCloseWorker(workerAddr string) {
	args := CloseWorkerArgs{}
	reply := CloseWorkerReply{}
	log.Printf("CloseWorker调用 addr:%s, args:%v \n", workerAddr, args)
	ok := callWorker(workerAddr, "WorkerInfo.CloseWorker", &args, &reply)
	if ok && reply.Success {
		return
	}
	log.Fatalf("CloseWorker调用失败 addr:%s, args:%v \n", workerAddr, args)
	return
}

func callWorker(workerAddr, rpcName string, args interface{}, reply interface{}) bool {
	defer func() {
		anyError := recover()
		if anyError != nil {
			log.Printf("call workers error:%v", anyError)
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
		log.Printf("workers: %s close error:%v", workerAddr, closeErr)
		return false
	}

	if err == nil {
		return true
	}

	log.Printf("call rpc error:%v", err.Error())
	return false
}
