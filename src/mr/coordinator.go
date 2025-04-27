package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.

	MappingTaskMap map[string]Task
	ReducingTaskMap map[string]Task

}

const (
	Unstart = iota
	Running
	Finished
)

const (
	MapTask = iota
	ReduceTask
)

type Task struct {
	Status int
	Name string
	TaskType int
	TaskNumber int
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

func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	for k, v := range c.MappingTaskMap {
		if v.Status == Unstart {
			v.Status = Running
			reply.Task = v
			reply.Task.Name = k
			c.MappingTaskMap[k] = v
			return nil
		}
	}
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
	c.MappingTaskMap = make(map[string]Task)
	c.ReducingTaskMap = make(map[string]Task)
	for idx, f := range files {
		c.MappingTaskMap[f] = Task{
			Status: Unstart,
			TaskNumber: idx,
		}
	}


	c.server()
	return &c
}
