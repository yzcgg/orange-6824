package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"

	"github.com/davecgh/go-spew/spew"
)


type Coordinator struct {
	// Your definitions here.

	MappingTaskMap []Task
	ReducingTaskMap map[string]Task

	M int
	R int
	Stage int
	curMapTaskId int
	curReduceTaskId int


}

// task status
const (
	Idle = iota
	Running
	Completed
)

// stage
const (
	Map = iota
	Reduce
	Done
)

type Task struct {
	Id int
	Status int
	WorkerId int
	InputFile string
	BeginTime time.Time
	Stage int
	NReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	switch c.Stage {
	case Map:
		if args.WorkerId == -1 {
			args.WorkerId = c.curMapTaskId
			c.curMapTaskId++
		}
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
		reply.Task.Id = -1
		return nil
	case Reduce:
	case Done:
		reply.Task.Stage = Done
	}
	log.Println("No available map task")
	return nil
}

func (c *Coordinator) GetNReduce(args *GetNReduceArgs, reply *GetNReduceReply) error {
	reply.NReduce = c.NReduce
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.M = len(files)
	c.R = nReduce
	c.Stage = Map
	c.MappingTaskMap = make([]Task, len(files))
	for idx, f := range files {
		c.MappingTaskMap[idx] = Task{
			Status: Idle,
			InputFile: f,
			Id: idx,
			Stage: c.Stage,
			NReduce: c.R,
		}
	}
	


	c.server()
	return &c
}
