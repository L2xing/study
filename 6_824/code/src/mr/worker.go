package mr

import (
	"fmt"
	"hash/fnv"
	"log"
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
	worker := &WorkerInfo{name: "worker", mapf: mapf, reducef: reducef, lock: &sync.Mutex{}, working: false}

	// 2. 向server注册
	go func() {
		for {
			CallRegister(worker.name)
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
	name    string
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
	shuffleFilePrefix := w.DoMap(fileName, args.NReduce)

	reply.Shuffle = shuffleFilePrefix
	reply.Success = true
	return nil
}

func (w *WorkerInfo) apply() bool {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.working {
		log.Printf("WorkerInfo.MapReq addr:%s 正在工作中，不处理\n", "12")
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
			log.Printf("WorkerInfo.ReduceReq error:%v\n", anyError)
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
func (w *WorkerInfo) MapReduce() {
	for {
		taskReply := CallApplyTask()
		switch taskReply.Command {
		case -1:
			log.Printf("worker退出\n")
			os.Exit(0)
		case 0:
			log.Printf("worker空闲\n")
			time.Sleep(1 * time.Second)
		case 1:
			shuffles := w.DoMap(taskReply.MapFileName, taskReply.NReduce)
			CallMapDone(shuffles)
			log.Printf("worker开始Map\n")
		case 2:
			log.Printf("worker开始Reduce\n")
		default:
			log.Fatalf("异常命令\n")
		}
	}
}

func CallMapDone(shuffles string) {

}

func (w *WorkerInfo) DoMap(fileName string, nReduce int) string {
	// 1. 读取file
	fileContent, _ := ReadFile(fileName)

	// 2. 调用map
	kvs := w.mapf(fileName, fileContent)
	log.Printf("map完成 fileName:%s \n", fileName)

	// 3. 创建shuffle
	// 3.1 kv结果分组
	groupKVs := make([][]KeyValue, nReduce)
	for idx := range groupKVs {
		groupKVs[idx] = make([]KeyValue, 0)
	}

	for _, kv := range kvs {
		key := kv.Key
		hashI := ihash(key) % nReduce
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

	return shuffleFilePrefix
}

func CallApplyTask() *ApplyTaskReply {
	args := ApplyTaskArgs{}
	reply := ApplyTaskReply{}
	// send the RPC request, wait for the reply.
	call("Coordinator.ApplyTask", &args, &reply)
	if !reply.Success {
		log.Printf("reply fail:%v \n", reply)
		return &ApplyTaskReply{Command: 0}
	}
	log.Printf("reply:%v \n", reply)
	return &reply
}

func CallRegister(addr string) {
	// declare an argument structure.
	args := RegisterArgs{}

	// fill in the argument(s).
	args.Msg = addr

	// declare a reply structure.
	reply := RegisterReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Register", &args, &reply)

	log.Printf("reply:%v \n", reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		log.Printf("call fail:%v \n", err)
	}
	return err == nil
}
