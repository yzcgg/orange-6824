package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"encoding/json"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
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
	task, ok := CallGetMapTask()
	if !ok {
		fmt.Println("Failed to get map task")
		return
	}

	file, err := os.Open(task.Name)
	if err != nil {
		log.Fatalf("cannot open %v", task.Name)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Name)
	}
	file.Close()
	kva := mapf(task.Name, string(content))

	interResJson, _ := json.Marshal(kva)
	interResFile, err := os.Create(fmt.Sprintf("mr-%d-%d.json", task.TaskNumber, task.TaskNumber))
	if err != nil {
		log.Fatalf("cannot create %v", task.Name+"_intermediate")
	}
	defer interResFile.Close()
	_, err = interResFile.Write(interResJson)
	if err != nil {
		log.Fatalf("cannot write to %v", task.Name+"_intermediate")
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallGetMapTask() (Task, bool) {
	args := GetMapTaskArgs{}

	reply := GetMapTaskReply{}

	ok := call("Coordinator.GetMapTask", &args, &reply)
	if ok {
		return reply.Task, true
	} else {
		return Task{}, false
	}
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
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
