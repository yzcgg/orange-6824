package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

var (
	workerId int = -1
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func ProcessMapTask(task *Task, mapf func(string, string) []KeyValue) []string {
	filename := task.InputFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	kvaPartition := Partition(kva, task.NReduce)
	resp := make([]string, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		curKva := kvaPartition[i]
		interResJson, _ := json.Marshal(curKva)
		interResFile, err := os.CreateTemp(".", fmt.Sprintf("mr-%d-%d.tmp", task.Id, i))
		if err != nil {
			log.Fatalf("cannot create %v", filename)
		}
		defer interResFile.Close()
		_, err = interResFile.Write(interResJson)
		if err != nil {
			log.Fatalf("cannot write to %v", filename)
		}
		resp[i] = interResFile.Name()
	}
	return resp
}

func ProcessReduceTask(task *Task, reducef func(string, []string) string) string {
	kvaList := make([]KeyValue, 0)
	for _, file := range task.InputFiles {
		log.Printf("processing %s\n", file)
		intermediateFile, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		defer intermediateFile.Close()
		content, err := io.ReadAll(intermediateFile)
		if err != nil {
			log.Fatalf("cannot read %v", file)
		}
		var kva []KeyValue
		if err := json.Unmarshal(content, &kva); err != nil {
			log.Fatalf("cannot unmarshal %v", file)
		}
		kvaList = append(kvaList, kva...)
	}
	sort.Sort(ByKey(kvaList))
	oname := fmt.Sprintf("mr-out-%d", task.Id)
	ofile, err := os.CreateTemp(".", oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
	}
	defer ofile.Close()
	i := 0
	for i < len(kvaList) {
		j := i + 1
		for j < len(kvaList) && kvaList[j].Key == kvaList[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvaList[k].Value)
		}
		output := reducef(kvaList[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvaList[i].Key, output)

		i = j
	}
	return ofile.Name()
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
		workerId = task.WorkerId
		if task.Id == -1 {
			log.Printf("no task available")
			time.Sleep(time.Second)
			continue
		}
		switch task.Stage {
		case Map:
			log.Printf("in map stage")
			intermediateFiles := ProcessMapTask(task, mapf)
			// notify coordinator that the task is finished
			CallFinishMapTask(intermediateFiles, task)
		case Reduce:
			log.Printf("in reduce stage")
			outputFile := ProcessReduceTask(task, reducef)

			CallFinishReduceTask(outputFile, task)
		case Done:
			log.Printf("Done!")
			return
		default:
			log.Fatalf("unknown stage")
		}

	}
	// Your worker implementation here.

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

func CallFinishMapTask(intermediateFiles []string, task *Task) {
	args := FinishMapTaskArgs{
		IntermediateFiles: intermediateFiles,
		Task:              *task,
	}
	reply := FinishMapTaskReply{}
	ok := call("Coordinator.FinishMapTask", &args, &reply)
	if ok {
		return
	} else {
		log.Fatalf("call finish task failed")
	}
}

func CallFinishReduceTask(outputFile string, task *Task) {
	args := FinishReduceTaskArgs{
		OutputFile: outputFile,
		Task:       *task,
	}
	reply := FinishMapTaskReply{}
	ok := call("Coordinator.FinishReduceTask", &args, &reply)
	if ok {
		return
	} else {
		log.Fatalf("call finish task failed")
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
