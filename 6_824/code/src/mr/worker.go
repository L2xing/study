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
	worker := &WorkerInfo{mapf: mapf, reducef: reducef}
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
}

func (w *WorkerInfo) MapReq(args *MapReqArgs, reply *MapReqReply) error {
	log.Printf("WorkerInfo.MapReq(%v)", args)

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
		_, err := fmt.Fprintf(file, "%s %s\n", kv.Key, kv.Value)
		if err != nil {
			continue
		}
	}
}

func (w *WorkerInfo) ReduceReq(args *ReduceReqArgs, reply *ReduceReqReply) error {
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
	fmt.Printf("reply.Success %v\n", reply.Success)
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
		log.Println("coordinator can not connect")
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
	fmt.Println(err)
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
