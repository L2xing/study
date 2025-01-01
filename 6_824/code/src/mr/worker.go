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
	log.Println("cli-server启动中")
	worker := &WorkerInfo{State: 0, Lock: &sync.Mutex{}, mapf: mapf, reducef: reducef}
	cliserver(worker)
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
	State   int
	Lock    *sync.Mutex
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (w *WorkerInfo) MapReq(args *MapReqArgs, reply *MapReqReply) error {
	log.Println("Map调用接受")
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
	go func(fileName string) {
		defer func() {
			if r := recover(); r != nil {
				log.Println("Recovered in WorkerInfo", r)
			}
		}()
		log.Printf("map携程 worker:%v addr1:%s, addr2:%s\n", w, w.Addr, (*w).Addr)
		shuffles, _ := w.handlerMapper(fileName)
		CallMapDone(w.Addr, fileName, shuffles)
	}(args.FileName)
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
	w.handlerReducer(args.ReducerKey, args.Shuffles)
	return nil
}

func (w *WorkerInfo) handlerMapper(fileName string) (map[string][]string, error) {
	// 1. 开启readfile
	contentByte, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("worker读取fileName失败。 fileName:%s, err:%v", fileName, err)
		return nil, err
	}
	content := string(contentByte)

	// 2. 执行mapper
	log.Printf("fileName:%s w:%s \n", fileName, w.mapf == nil)
	kvs := w.mapf(fileName, content)
	log.Printf("kvs:%d \n", len(kvs))
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
	log.Printf("map done, fileName: %s  \n", fileName)

	// 4. 通知coordinator
	return shuffleMap, nil
}

func (w *WorkerInfo) handlerReducer(name string, shuffles []string) {
	if len(shuffles) == 0 {
		return
	}
	output := "mr-out-" + name
	log.Printf("output:%s \n", output)
	createFile, err := os.Create("mr-out-" + name)
	if err != nil {
		return
	}
	reducef := w.reducef(name, shuffles)
	createFile.WriteString(reducef)
	createFile.Close()
	//CallReduceDone(name, output)
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
func cliserver(worker *WorkerInfo) {
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
