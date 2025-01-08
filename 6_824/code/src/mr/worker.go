package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the ReducerKey
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// 1. 启动一个server
	log.Println("cli-server启动中")
	worker := &WorkerInfo{mapf: mapf, reducef: reducef, lock: &sync.Mutex{}, working: false}
	workerServer(worker)
	log.Println("cli-server启动，name=" + worker.Addr)

	// 2. 向server注册
	go func() {
		for {
			CallRegister(worker.Addr)
			time.Sleep(1 * time.Second)
		}
	}()

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		time.Sleep(1 * time.Second)
	}
}

type WorkerInfo struct {
	Addr    string
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	working bool
	lock    *sync.Mutex
}

func (w *WorkerInfo) MapReq(args *MapReqArgs, reply *MapReqReply) error {
	defer func() {
		anyError := recover()
		if anyError != nil {
			log.Printf("WorkerInfo.MapReq error:%v\n", anyError)
		}
	}()
	w.apply()
	defer w.release()

	log.Printf("WorkerInfo.MapReq(%v)\n", args)

	// 1. 读取file
	fileName := args.FileName
	fileContent, _ := ReadFile(fileName)

	// 2. 调用map
	kvs := w.mapf(fileName, fileContent)
	log.Printf("map完成 fileName:%s \n", fileName)

	// 3. 创建shuffle
	// 3.1 kv结果分组
	groupKVs := make([][]KeyValue, args.NReduce)
	for idx := range groupKVs {
		groupKVs[idx] = make([]KeyValue, 0)
	}

	for _, kv := range kvs {
		key := kv.Key
		hashI := ihash(key) % args.NReduce
		groupKVs[hashI] = append(groupKVs[hashI], kv)
	}
	// todo shuffle的文件生成方式可能会hash碰撞
	shuffleFilePrefix := "map_out_" + strconv.Itoa(ihash(fileName)) + "_"
	for idx, kvs := range groupKVs {
		if len(kvs) == 0 {
			continue
		}
		shuffleFile := shuffleFilePrefix + strconv.Itoa(idx)
		CreateShuffleFile(shuffleFile, kvs)
	}

	reply.Shuffle = shuffleFilePrefix
	reply.Success = true
	return nil
}

func (w *WorkerInfo) apply() bool {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.working {
		log.Printf("WorkerInfo.MapReq addr:%s 正在工作中，不处理\n", w.Addr)
		return false
	} else {
		w.working = true
		return true
	}
}

func (w *WorkerInfo) release() {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.working = false
}

func CreateShuffleFile(fileName string, kvs []KeyValue) {
	if len(kvs) == 0 {
		return
	}

	// 1. 创建文件
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("文件创建失败，终止服务。 file:%s,", fileName)
	}
	defer file.Close()

	// 2. 追加内容
	for _, kv := range kvs {
		// todo 这个可能会遇到key或value中存在空格的数据
		_, err := fmt.Fprintf(file, "%s %s\n", kv.Key, kv.Value)
		if err != nil {
			continue
		}
	}
}

func (w *WorkerInfo) ReduceReq(args *ReduceReqArgs, reply *ReduceReqReply) error {
	defer func() {
		anyError := recover()
		if anyError != nil {
			log.Printf("WorkerInfo.MapReq error:%v\n", anyError)
		}
	}()
	w.apply()
	defer w.release()

	log.Printf("WorkerInfo.ReduceReq(%v)\n", args)
	// 1. 将文件读入内存中
	shuffles := make([]string, 0)
	hashi := args.HashI
	for _, shufflePrefix := range args.Shuffles {
		shuffle := shufflePrefix + strconv.Itoa(hashi)
		shuffles = append(shuffles, shuffle)
	}

	shuffleMaps := make(map[string][]string, 0)
	for _, shuffle := range shuffles {
		content, _ := ReadFile(shuffle)
		lineStrArray := strings.Split(content, "\n")
		for _, lineStr := range lineStrArray {
			kv := strings.Split(lineStr, " ")
			if len(kv) != 2 {
				continue
			}
			_, ok := shuffleMaps[kv[0]]
			if !ok {
				shuffleMaps[kv[0]] = make([]string, 0)
			}
			shuffleMaps[kv[0]] = append(shuffleMaps[kv[0]], kv[1])
		}
	}

	// 2. 调用reducer
	reduceOutPut := "mr-out-" + strconv.Itoa(hashi)
	file, _ := os.Create(reduceOutPut)
	for reduceKey, reduceValues := range shuffleMaps {
		reduceResult := w.reducef(reduceKey, reduceValues)
		fmt.Fprintf(file, "%s %s\n", reduceKey, reduceResult)
	}

	reply.Success = true
	reply.HashI = hashi
	reply.OutPutFile = reduceOutPut
	return nil
}

func (w *WorkerInfo) CloseWorker(args *CloseWorkerArgs, reply *CloseWorkerReply) string {
	go func() {
		time.Sleep(time.Second * 2)
		log.Println("worker关闭")
		os.Exit(0)
	}()
	log.Printf("WorkerInfo.CloseWorker(%v)", args)
	reply.Success = true
	return ""
}

/**
 * 读取文件内容
 */
func ReadFile(fileName string) (string, error) {
	contentByte, err := os.ReadFile(fileName)
	if err != nil {
		log.Printf("worker读取fileName失败。 fileName:%s, err:%v \n", fileName, err)
		return "", err
	}

	if len(contentByte) == 0 {
		log.Printf("fileName:%s 文件内容为空 \n", fileName)
		return "", nil
	}

	content := string(contentByte)
	return content, nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

func CallRegister(addr string) {
	// declare an argument structure.
	args := RegisterArgs{}

	// fill in the argument(s).
	args.Addr = addr

	// declare a reply structure.
	reply := RegisterReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Register", &args, &reply)

	// reply.Y should be 100.
	log.Printf("reply.Success %v\n", reply.Success)
}

func CallMapDone(addr string, fileName string, shuffles map[string][]string) {
	args := MapDoneArgs{}
	args.Addr = addr
	args.FileName = fileName
	args.Shuffles = shuffles
	reply := MapDoneReply{}
	log.Printf("CallMapDone started. addr:%v, fileName:%v \n", args.Addr, args.FileName)
	call("Coordinator.MapDone", &args, &reply)
	log.Printf("CallMapDone reply. reply:%v \n", reply.Success)
	if reply.Success == false {
		time.Sleep(1 * time.Second)
		log.Printf("CallMapDone Retry. fileName:%s \n", fileName)
		CallMapDone("", fileName, shuffles)
	}
	log.Printf("CallMapDone finished. fileName:%s \n", fileName)
}

func CallReduceDone(name string, output string) {
	args := ReduceDoneArgs{ShuffleName: name, Result: output}
	reply := ReduceDoneReply{}
	call("Coordinator.ReduceDone", &args, &reply)

	if reply.Success == false {
		time.Sleep(1 * time.Second)
		CallReduceDone(name, output)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatalf("coordinator can not connect")
		return false
	}
	log.Printf("调用master rpcName:%s \n", rpcname)
	err = c.Call(rpcname, args, reply)

	closeError := c.Close()
	if closeError != nil {
		log.Println("close error:", closeError)
		return false
	}

	if err == nil {
		return true
	}
	log.Printf("err: %v \n", err)
	return false
}

// start a thread that listens for RPCs from worker.go
func workerServer(worker *WorkerInfo) {
	l, e := net.Listen("tcp", ":0")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	addr := l.Addr().String()
	worker.Addr = addr
	rpc.Register(worker)
	rpc.HandleHTTP()
	go http.Serve(l, nil)
}
