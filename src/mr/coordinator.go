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
	aliveWorkerMap    map[int]struct{}
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
	Finish
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

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	c.lock.Lock()
	defer c.lock.Unlock()

	// 记录存活的 worker
	if _, ok := c.aliveWorkerMap[args.WorkerId]; !ok {
		c.aliveWorkerMap[args.WorkerId] = struct{}{}
	}

	switch c.Stage {
	case Map:
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
	case Finish:
		reply.Task = Task{
			Stage: Finish,
		}
		delete(c.aliveWorkerMap, args.WorkerId)
	}
	return nil
}

func (c *Coordinator) FinishMapTask(args *FinishMapTaskArgs, reply *FinishMapTaskReply) error {
	c.lock.Lock()
	if c.MappingTaskMap[args.Task.Id].Status == Running && c.MappingTaskMap[args.Task.Id].WorkerId == args.Task.WorkerId {
		c.MappingTaskMap[args.Task.Id].Status = Completed
		c.completedMapTaskCount++
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
		// log.Printf("worker %d finished map task %d\n", args.Task.WorkerId, args.Task.Id)
	}

	if c.completedMapTaskCount == c.M {
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
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) FinishReduceTask(args *FinishReduceTaskArgs, reply *FinishReduceTaskReply) error {
	c.lock.Lock()
	if c.ReducingTaskMap[args.Task.Id].Status == Running && c.ReducingTaskMap[args.Task.Id].WorkerId == args.Task.WorkerId {
		c.ReducingTaskMap[args.Task.Id].Status = Completed
		c.completedReduceTaskCount++
		err := os.Rename(args.OutputFile, fmt.Sprintf("mr-out-%d", args.Task.Id))
		if err != nil {
			log.Fatalf("rename file %s failed", args.OutputFile)
		}
	}

	if c.completedReduceTaskCount == c.R {
		c.Stage = Finish
	}
	c.lock.Unlock()

	return nil
}

func (c *Coordinator) GetWorkerId(args *GetWorkerIdArgs, reply *GetWorkerIdReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.avaliableWorkerId++
	reply.WorkerId = c.avaliableWorkerId
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
	c.lock.Lock()
	defer c.lock.Unlock()
	return len(c.aliveWorkerMap) == 0 && c.Stage == Finish
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
	c.aliveWorkerMap = make(map[int]struct{})
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

	go c.StartTaskQueueChecker(1 * time.Second)

	c.server()
	return &c
}

// StartTaskQueueChecker 启动定时任务队列检查器
func (c *Coordinator) StartTaskQueueChecker(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		c.lock.Lock()
		switch c.Stage {
		case Map:
			for idx, task := range c.MappingTaskMap {
				if task.Status == Running && time.Since(task.BeginTime) > 10*time.Second {
					c.MappingTaskMap[idx].Status = Idle
					c.MappingTaskMap[idx].WorkerId = -1
					delete(c.aliveWorkerMap, task.WorkerId) // 超过10s未完成任务，视为worker宕机
					// log.Printf("Map task %d is timeout, reset to idle", task.Id)
				}
			}
		case Reduce:
			for idx, task := range c.ReducingTaskMap {
				if task.Status == Running && time.Since(task.BeginTime) > 10*time.Second {
					c.ReducingTaskMap[idx].Status = Idle
					c.ReducingTaskMap[idx].WorkerId = -1
					delete(c.aliveWorkerMap, task.WorkerId) // 超过10s未完成任务，视为worker宕机
					// log.Printf("Reduce task %d is timeout, reset to idle", task.Id)
				}
			}
		case Finish:
			c.lock.Unlock()
			return
		}
		c.lock.Unlock()
	}
}
