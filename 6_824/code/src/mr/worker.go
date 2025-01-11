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
			//CallRegister(worker.name)
			worker.MapReduce()
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
		_, err = fmt.Fprintf(file, "%s %s\n", kv.Key, kv.Value)
		if err != nil {
			log.Fatalf("Shuffle file err:%v", err)
		}
	}
}

func (w *WorkerInfo) DoReduce(hashI int, shuffles []string) string {
	// 1. 将文件读入内存中
	for idx, shufflePrefix := range shuffles {
		shuffle := shufflePrefix + strconv.Itoa(hashI)
		shuffles[idx] = shuffle
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
	reduceOutPut := "mr-out-" + strconv.Itoa(hashI)
	file, _ := os.Create(reduceOutPut)
	for reduceKey, reduceValues := range shuffleMaps {
		reduceResult := w.reducef(reduceKey, reduceValues)
		_, err := fmt.Fprintf(file, "%s %s\n", reduceKey, reduceResult)
		if err != nil {
			log.Fatalf("Reduce file err:%v", err)
		}
	}

	return reduceOutPut
}

/**
 * 读取文件内容
 */
func ReadFile(fileName string) (string, error) {
	contentByte, err := os.ReadFile(fileName)
	if err != nil {
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
			time.Sleep(1 * time.Second)
		case 1:
			log.Printf("worker开始Map\n")
			shuffles := w.DoMap(taskReply.MapFileName, taskReply.NReduce)
			CallMapDone(taskReply.MapFileName, shuffles)
		case 2:
			log.Printf("worker开始Reduce\n")
			output := w.DoReduce(taskReply.ReduceIdx, taskReply.Shuffles)
			CallReduceDone(taskReply.ReduceIdx, output)
		default:
			log.Fatalf("异常命令\n")
		}
	}
}

func CallReduceDone(idx int, output string) {
	args := ReduceDoneArgs{idx, true}
	reply := ReduceDoneReply{}
	success := call("Coordinator.ReduceDone", &args, &reply)
	if !success || !reply.Success {
		log.Fatalf("Coordinator.ReduceDone FAIL:%v\n", reply)
	}
}

func CallMapDone(fileName, shuffles string) {
	args := MapDoneArgs{FileName: fileName, Shuffles: shuffles}
	reply := MapDoneReply{}
	success := call("Coordinator.MapDone", &args, &reply)
	if !success || !reply.Success {
		log.Fatalf("Coordinator.MapDone FAIL:%v\n", reply)
	}

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
	shuffleFilePrefix := "map_out_" + strconv.FormatInt(time.Now().Unix(), 10) + "_" + strconv.Itoa(ihash(fileName)) + "_"
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
	return &reply
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
