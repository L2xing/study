package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int

	// mapper
	mappers     map[string]string
	mappersChan chan MapperResult
	mapperDone  chan bool

	// ReducerKey
	reducers     []bool
	reducersChan chan ReducerResult
	reduceDone   chan bool

	reducerFilePrefix string

	// workers
	wLock   sync.Mutex
	workers []string
	curIdx  int

	// ret
	retL sync.Mutex
	ret  bool
}

type MapperResult struct {
	FileName string
	Shuffle  string
	success  bool
}

type ReducerResult struct {
	idx     int
	success bool
}

// Your code here -- RPC handlers for the workers to call.
func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	log.Printf("Register: %s\n", args.Msg)
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
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go http.Serve(l, nil)
	WriteCoordinatorSock(sockname)
}

func WriteCoordinatorSock(sockname string) {
	socknameFile := CoordinatorSockFile
	os.Remove(socknameFile)

	create, _ := os.Create(socknameFile)
	create.WriteString(sockname)
	create.Close()
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
	c.mappersChan = make(chan MapperResult, 1)
	c.mapperDone = make(chan bool)
	for _, file := range files {
		c.mappers[file] = ""
	}

	// 2. 初始化reducer
	c.reducers = make([]bool, nReduce)
	c.reducersChan = make(chan ReducerResult, 1)
	c.reduceDone = make(chan bool, 1)
	for idx := range c.reducers {
		c.reducers[idx] = false
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
		go MapReq(c, fileName)
	}

	go func(c *Coordinator) {
		for {
			mr := <-c.mappersChan
			if !mr.success {
				log.Printf("MapReq: %s 失败重试\n", mr.FileName)
				go MapReq(c, mr.FileName)
				continue
			}

			fileName := mr.FileName
			shuffle := mr.Shuffle
			c.mappers[fileName] = shuffle
			log.Printf("MapReq: %s 完成, shuffle: %s\n", fileName, shuffle)

			// 2. 检查是否全部完成
			allDone := true
			for _, shuffle := range c.mappers {
				if len(shuffle) == 0 {
					allDone = false
					break
				}
			}
			if allDone {
				break
			}
		}

		log.Printf("MapDone!\n")
		c.mapperDone <- true
	}(c)

	// 2. 循环直到Map全部处理完毕
	<-c.mapperDone

	// 3. 开启reducer阶段
	for idx := range c.reducers {
		go ReduceReq(c, idx)
	}

	go func(c *Coordinator) {
		for {
			rr := <-c.reducersChan
			if !rr.success {
				log.Printf("ReduceReq: %v 失败重试\n", rr.idx)
				go ReduceReq(c, rr.idx)
				continue
			}

			idx := rr.idx
			c.reducers[idx] = true

			// 4. 检查是否全部完成
			allDone := true
			for _, done := range c.reducers {
				if !done {
					allDone = false
					break
				}
			}
			if allDone {
				break
			}
		}
		log.Printf("ReduceDone!\n")
		c.reduceDone <- true
	}(c)

	// 4. reducer验证
	<-c.reduceDone

	// 5. 通知coordinator完成
	c.retL.Lock()
	defer c.retL.Unlock()
	c.ret = true
	log.Printf("All Done!\n")
}

func ReduceReq(c *Coordinator, idx int) {
	// 1. 申请worker
	worker := c.applyWorker()
	for strings.Compare(worker, "") == 0 {
		worker = c.applyWorker()
	}

	// 2. 合并shuffles
	shuffles := make([]string, 0)
	for _, shuffle := range c.mappers {
		shuffles = append(shuffles, shuffle)
	}

	// 4. 提交任务
	CallFuncInTime(5*time.Second,
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Recovered in ReduceReq: %v", r)
				}
			}()
			reduceDone := CallReduceReq(worker, idx, shuffles)
			c.reducersChan <- ReducerResult{idx, reduceDone}
		},
		func() {
			log.Printf("ReduceReq: %v 调用超时", idx)
			c.reducersChan <- ReducerResult{idx, false}
		})
}

func MapReq(c *Coordinator, fileName string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered in MapReq: %v", r)
		}
	}()

	// 1. 申请worker
	worker := c.applyWorker()
	for strings.Compare(worker, "") == 0 {
		worker = c.applyWorker()
	}

	// 3. 提交任务
	CallFuncInTime(5*time.Second,
		func() {
			shuffle, ok := CallMapReq(worker, fileName, c.nReduce)
			c.mappersChan <- MapperResult{fileName, shuffle, ok}
		},
		func() {
			log.Printf("MapReq: %v 调用超时", fileName)
			c.mappersChan <- MapperResult{fileName, "", false}
		})
}

func CallFuncInTime(d time.Duration, f func(), timeoutFunc func()) {
	after := time.After(d)
	anies := make(chan bool)
	go func() {
		f()
		anies <- true
	}()
	select {
	case <-after:
		timeoutFunc()
	case <-anies:
		return
	}
}

func (c *Coordinator) applyWorker() string {
	c.wLock.Lock()
	defer c.wLock.Unlock()
	if len(c.workers) == 0 {
		log.Println("no workers")
		time.Sleep(500 * time.Millisecond)
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
	log.Printf("CloseWorker调用失败 addr:%s, args:%v \n", workerAddr, args)
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
