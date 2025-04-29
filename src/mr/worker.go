package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

var (
	workerId int = -1
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

func ProcessMapTask(task *Task, mapf func(string, string) []KeyValue) {
	filename := task.InputFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	kvaPartition := Partition(kva, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		curKva := kvaPartition[i]
		interResJson, _ := json.Marshal(curKva)
		interResFile, err := os.CreateTemp(".", fmt.Sprintf("mr-%d-%d.tmp", task.Id, i))
		if err!= nil {
			log.Fatalf("cannot create %v", filename)
		}
		defer interResFile.Close()
		_, err = interResFile.Write(interResJson)
		if err!= nil {
			log.Fatalf("cannot write to %v", filename)
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		task := CallGetTask(workerId)
		if task == nil {
			log.Fatalf("get task failed")
			break
		}
		if task.Id == -1 {
			log.Printf("no task available")
			time.Sleep(time.Second)
			continue
		}
		switch task.Stage {
		case Map:
			log.Printf("in map stage")

		case Reduce:
			log.Printf("in reduce stage")
		case Done:
			log.Printf("Done!")
			return
		default:
			log.Fatalf("unknown stage")
		}

	}
	// Your worker implementation here.

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

	nReduce, ok := CallGetNReduce()

	kvaPartition := Partition(kva, nReduce)

	for i := 0; i < nReduce; i++ {
		curKva := kvaPartition[i]
		interResJson, _ := json.Marshal(curKva)
		interResFile, err := os.OpenFile(fmt.Sprintf("mr-%d-%d.json", task.TaskNumber, i), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("cannot create %v", task.Name+"_intermediate")
		}
		defer interResFile.Close()
		_, err = interResFile.Write(interResJson)
		if err != nil {
			log.Fatalf("cannot write to %v", task.Name+"_intermediate")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func Partition(kva []KeyValue, nReduce int) [][]KeyValue {
	partition := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		partition[ihash(kv.Key)%nReduce] = append(partition[ihash(kv.Key)%nReduce], kv)
	}
	return partition
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

func CallGetTask(workerId int) *Task {
	args := GetTaskArgs{
		WorkerId: workerId,
	}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return &reply.Task
	} else {
		return nil
	}
}

func CallGetNReduce() (int, bool) {
	args := GetNReduceArgs{}

	reply := GetNReduceReply{}

	ok := call("Coordinator.GetNReduce", &args, &reply)
	if ok {
		return reply.NReduce, true
	} else {
		return 0, false
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
