package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
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
	fmt.Println("cli-server启动中")
	addr := cliserver()
	fmt.Println("cli-server启动，name=" + addr)

	// 2. 向server注册
	go func() {
		for {
			CallRegister(addr)
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
	State   int
	Lock    *sync.Mutex
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (w *WorkerInfo) MapReq(args *MapReqArgs, reply *MapReqReply) error {
	fmt.Println("Map调用接受")
	w.Lock.Lock()
	defer w.Lock.Unlock()

	if w.State != 0 {
		reply.Success = false
		return nil
	}

	// in mapper
	w.State = 1
	reply.Success = true

	// 开启携程开始处理mapper
	go func() {
		w.handlerMapper(args.FileName)
	}()
	return nil
}

func (w *WorkerInfo) ReduceReq(args *ReduceReqArgs, reply *ReduceReqReply) error {
	fmt.Println("Reduce调用接受")
	w.Lock.Lock()
	defer w.Lock.Unlock()
	if w.State != 0 {
		reply.Success = false
		return nil
	}

	w.State = 1
	reply.Success = true

	// 开启携程开始处理reduce
	go func() {
		w.handlerReducer(args.ReducerKey, args.Shuffles)
	}()
	return nil
}

func (w *WorkerInfo) handlerMapper(fileName string) {
	// 1. 开启readfile
	contentByte, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatal(err)
	}
	content := string(contentByte)

	// 2. 执行mapper
	kvs := w.mapf(fileName, content)

	// 3. 存入shuffle
	shuffleMap := make(map[string][]string)
	if len(kvs) > 0 {
		for _, kv := range kvs {
			key := kv.Key
			value := kv.Value
			shuffles, exist := shuffleMap[key]
			if exist {
				shuffles = append(shuffles, value)
			} else {
				shuffles = []string{value}
			}
			shuffleMap[key] = shuffles
		}
	}
	w.State = 0

	// 4. 通知coordinator
	CallMapDone(fileName, shuffleMap)
}

func (w *WorkerInfo) handlerReducer(name string, shuffles []string) {
	if len(shuffles) == 0 {
		return
	}
	output := "mr-out-" + name
	createFile, err := os.Create("mr-out-" + name)
	if err != nil {
		return
	}
	reducef := w.reducef(name, shuffles)
	createFile.WriteString(reducef)
	createFile.Close()

	CallReduceDone(name, output)
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

func CallMapDone(fileName string, shuffles map[string][]string) {
	args := MapDoneArgs{}
	args.FileName = fileName
	args.Shuffles = shuffles
	reply := MapDoneReply{}
	call("Coordinator.MapDone", &args, &reply)

	for reply.Success == false {
		time.Sleep(1 * time.Second)
		CallMapDone(fileName, shuffles)
	}
}

func CallReduceDone(name string, output string) {
	args := ReduceDoneArgs{ShuffleName: name, Result: output}
	reply := ReduceDoneReply{}
	call("Coordinator.ReduceDone", &args, &reply)

	for reply.Success == false {
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	fmt.Println("调用master rpcName:" + rpcname)
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// start a thread that listens for RPCs from worker.go
func cliserver() string {
	l, e := net.Listen("tcp", ":0")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	addr := l.Addr().String()
	worker := WorkerInfo{Addr: addr, State: 0, Lock: &sync.Mutex{}}
	rpc.Register(worker)
	rpc.HandleHTTP()
	go http.Serve(l, nil)
	return addr
}
