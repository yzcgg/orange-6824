package mr

import (
	"fmt"
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

type Coordinator struct {
	// Your definitions here.
	lock sync.Mutex

	MappingTaskMap       []Task
	ReducingTaskMap      []Task
	IntermediateFileAddr [][]string

	M                        int
	R                        int
	Stage                    int
	completedMapTaskCount    int
	completedReduceTaskCount int

	avaliableWorkerId int
}

// task status
const (
	Idle = iota
	Running
	Completed
)

// stage
const (
	_ = iota
	Map
	Reduce
	Done
)

type Task struct {
	Id         int
	Status     int
	WorkerId   int
	BeginTime  time.Time
	Stage      int
	NReduce    int
	InputFile  string
	InputFiles []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.lock.Lock()
	if args.WorkerId == -1 {
		args.WorkerId = c.avaliableWorkerId
		log.Printf("Assign a new worker id %d\n", args.WorkerId)
		c.avaliableWorkerId++
	}
	c.lock.Unlock()

	c.lock.Lock()
	defer c.lock.Unlock()
	switch c.Stage {
	case Map:
		log.Println("Current Stage is Map")
		// find idle task
		for idx, task := range c.MappingTaskMap {
			if task.Status == Idle {
				c.MappingTaskMap[idx].Status = Running
				c.MappingTaskMap[idx].WorkerId = args.WorkerId
				c.MappingTaskMap[idx].BeginTime = time.Now()
				reply.Task = c.MappingTaskMap[idx]
				return nil
			}
		}
		// can not find idle task
		reply.Task = Task{
			Id: -1,
		}
		return nil
	case Reduce:
		log.Println("Current Stage is Reduce")
		for idx, task := range c.ReducingTaskMap {
			if task.Status == Idle {
				c.ReducingTaskMap[idx].Status = Running
				c.ReducingTaskMap[idx].WorkerId = args.WorkerId
				c.ReducingTaskMap[idx].BeginTime = time.Now()
				c.ReducingTaskMap[idx].InputFiles = c.IntermediateFileAddr[idx]
				reply.Task = c.ReducingTaskMap[idx]
				return nil
			}
		}
	case Done:
		reply.Task = Task{
			Stage: Done,
		}
	}
	log.Println("No available task")
	return nil
}

func (c *Coordinator) FinishMapTask(args *FinishMapTaskArgs, reply *FinishMapTaskReply) error {
	for _, file := range args.IntermediateFiles {
		// commit
		filePrefix := strings.Split(strings.Split(file, ".")[1], "/")[1]
		intermediaFileName := fmt.Sprintf("%s.json", filePrefix)
		err := os.Rename(file, intermediaFileName)
		if err != nil {
			log.Fatalf("rename file %s failed", file)
		}
		reduceTaskNumber, _ := strconv.Atoi(strings.Split(filePrefix, "-")[2])
		c.IntermediateFileAddr[reduceTaskNumber] = append(c.IntermediateFileAddr[reduceTaskNumber], intermediaFileName)
	}
	c.lock.Lock()
	if c.MappingTaskMap[args.Task.Id].Status == Running {
		c.MappingTaskMap[args.Task.Id].Status = Completed
		c.completedMapTaskCount++
	}
	c.lock.Unlock()
	log.Printf("Map task %d is completed\n", args.Task.Id)
	if c.completedMapTaskCount == c.M {
		log.Println("All map tasks are completed")
		c.ReducingTaskMap = make([]Task, c.R)
		for i := 0; i < c.R; i++ {
			c.ReducingTaskMap[i] = Task{
				Id:         i,
				Status:     Idle,
				NReduce:    c.R,
				Stage:      Reduce,
				InputFiles: c.IntermediateFileAddr[i],
			}
		}
		c.Stage = Reduce
	}
	return nil
}

func (c *Coordinator) FinishReduceTask(args *FinishReduceTaskArgs, reply *FinishReduceTaskReply) error {
	err := os.Rename(args.OutputFile, args.OutputFile)
	if err != nil {
		log.Fatalf("rename file %s failed", args.OutputFile)
	}
	c.lock.Lock()
	if c.ReducingTaskMap[args.Task.Id].Status == Running {
		c.ReducingTaskMap[args.Task.Id].Status = Completed
		c.completedReduceTaskCount++
	}
	c.lock.Unlock()
	log.Printf("Reduce task %d is completed\n", args.Task.Id)
	if c.completedReduceTaskCount == c.R {
		log.Println("All reduce tasks are completed")
		c.Stage = Done
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.Stage == Done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.M = len(files)
	c.R = nReduce
	c.Stage = Map
	c.MappingTaskMap = make([]Task, len(files))
	for idx, f := range files {
		c.MappingTaskMap[idx] = Task{
			InputFile: f,
			Id:        idx,
			Status:    Idle,
			NReduce:   c.R,
			Stage:     c.Stage,
		}
	}
	c.IntermediateFileAddr = make([][]string, c.R)
	for i := 0; i < c.R; i++ {
		c.IntermediateFileAddr[i] = make([]string, 0)
	}

	c.server()
	return &c
}
