package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int
	apply   sync.Mutex

	// mapper
	mappers           map[string]*MapResult
	mappersParamChan  chan MapperParam
	mappersResultChan chan MapperResult
	mapperDone        chan bool

	// reducer
	reducers           []*ReduceResult
	reducersParamChan  chan ReducerParam
	reducersResultChan chan ReducerResult
	reducerDone        chan bool

	reducerFilePrefix string

	// ret
	retL sync.Mutex
	ret  bool
}

type MapResult struct {
	done    bool
	shuffle string
	timeout int64
}

type ReduceResult struct {
	done    bool
	timeout int64
}

type TaskReq struct {
	MapperParam
	ReducerParam
}

type TaskResult struct {
	mr MapperResult
	rr ReducerResult
}

type MapperParam struct {
	FileName string
	NReduce  int
}

type MapperResult struct {
	FileName string
	Shuffle  string
	success  bool
}

type ReducerParam struct {
	ReduceIdx int
	Shuffles  []string
}

type ReducerResult struct {
	idx     int
	success bool
}

// Your code here -- RPC handlers for the workers to call.
func (c *Coordinator) MapDone(args *MapDoneArgs, reply *MapDoneReply) error {
	log.Printf("MapDone Receive. args: %v\n", args)
	fileName := args.FileName
	shuffles := args.Shuffles
	c.mappersResultChan <- MapperResult{fileName, shuffles, true}
	reply.Success = true
	return nil
}

func (c *Coordinator) ReduceDone(args *ReduceDoneArgs, reply *ReduceDoneReply) error {
	log.Printf("ReduceDone Receive. args: %v\n", args)
	c.reducersResultChan <- ReducerResult{args.HashI, true}
	reply.Success = true
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
		log.Fatal("listenMapperResult error:", e)
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
	//c.taskChan = make(chan TaskReq, 1)

	// 1. 初始化mapper
	c.mappers = make(map[string]*MapResult)
	c.mappersParamChan = make(chan MapperParam, 1)
	c.mappersResultChan = make(chan MapperResult, 1)
	c.mapperDone = make(chan bool)
	for _, file := range files {
		c.mappers[file] = &MapResult{}
	}

	// 2. 初始化reducer
	c.reducers = make([]*ReduceResult, nReduce)
	c.reducersParamChan = make(chan ReducerParam, 1)
	c.reducersResultChan = make(chan ReducerResult, 1)
	c.reducerDone = make(chan bool, 1)
	for idx := range c.reducers {
		c.reducers[idx] = &ReduceResult{}
	}
	c.reducerFilePrefix = "mr-out"

	go c.listenerMapReducer()

	return &c
}

func (c *Coordinator) listenerMapReducer() {
	// 1. Mapper Done Listener
	go c.listenMapperResult()

	// 2. Reducer Done Listener
	go c.listenReducerResult()

	// 3. 开始投递Mapper
	go func(c *Coordinator) {
		for _, fileName := range c.files {
			c.mappersParamChan <- MapperParam{fileName, c.nReduce}
			log.Printf("MapReducer Listener...: %s\n", fileName)
		}
	}(c)

	// 2. Mapper
	<-c.mapperDone

	go func(c *Coordinator) {
		shuffles := c.getAllShuffles()
		for i := 0; i < c.nReduce; i++ {
			c.reducersParamChan <- ReducerParam{i, shuffles}
		}
	}(c)

	<-c.reducerDone

	// 5. 通知coordinator完成
	c.retL.Lock()
	defer c.retL.Unlock()
	c.ret = true
	log.Printf("All Done!\n")
}

func (c *Coordinator) getAllShuffles() []string {
	shuffles := make([]string, 0)
	for _, r := range c.mappers {
		shuffles = append(shuffles, r.shuffle)
	}
	return shuffles
}

func (c *Coordinator) listenMapperResult() {
	for {
		mr := <-c.mappersResultChan

		// 1. 异常重试
		if !mr.success {
			log.Printf("mapper retry error: %v\n", mr)
			c.mappersParamChan <- MapperParam{mr.FileName, c.nReduce}
			continue
		}

		// 2. shuffle存储
		fileName := mr.FileName
		shuffle := mr.Shuffle
		c.mappers[fileName] = &MapResult{true, shuffle, 0}
		log.Printf("MapReq: %s 完成, shuffle: %s\n", fileName, shuffle)

		// 3. 检查是否全部完成
		allDone := true
		for _, r := range c.mappers {
			if !r.done {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}
	}

	// 4. 处理完成
	log.Printf("MapDone!\n")
	c.mapperDone <- true
}

func (c *Coordinator) listenReducerResult() {
	for {
		rr := <-c.reducersResultChan

		// 1. 异常重试
		if !rr.success {
			log.Printf("reducer retry. error: %v\n", rr)
			shuffles := c.getAllShuffles()
			c.reducersParamChan <- ReducerParam{rr.idx, shuffles}
			continue
		}

		// 2. reducer标记
		idx := rr.idx
		c.reducers[idx] = &ReduceResult{true, 0}

		// 3. 检查是否全部完成
		allDone := true
		for _, rr := range c.reducers {
			if !rr.done {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}
	}

	// 4. 通知完成
	log.Printf("ReduceDone!\n")
	c.reducerDone <- true
}

func (c *Coordinator) ApplyTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	c.apply.Lock()
	defer c.apply.Unlock()
	reply.Success = true

	// 1. mapper 任务分发
	for fileName, r := range c.mappers {
		if r.done {
			continue
		}

		// 1. 短期内不允许重试
		timeout := r.timeout
		curTimestamp := time.Now().Unix()
		if timeout > curTimestamp {
			continue
		}
		r.timeout = curTimestamp + int64(time.Second*5)

		reply.Command = 1
		reply.MapFileName = fileName
		reply.NReduce = c.nReduce
		return nil
	}

	// 2. reducer 任务分发
	shuffles := c.getAllShuffles()
	for idx, rr := range c.reducers {
		if rr.done {
			continue
		}

		// 防止重试
		timeout := rr.timeout
		curTimestamp := time.Now().Unix()
		if timeout > curTimestamp {
			continue
		}
		rr.timeout = curTimestamp + int64(time.Second*5)

		reply.Command = 2
		reply.ReduceIdx = idx
		reply.Shuffles = shuffles
		return nil
	}

	if c.ret {
		reply.Command = -1
	}

	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
