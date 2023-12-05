package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	MapTaskChan    chan *Task
	ReduceTaskChan chan *Task
	NumReduce      int // reduce num
	NumMap         int // map num
	NumDoneReduce  int // reduce done num
	NumDoneMap     int // map done num
	State          int // map\reduce\done
	mu             sync.Mutex
	Timeout        time.Duration
	MapTasks       map[int]*Task
	ReduceTasks    map[int]*Task
}

type Task struct {
	TaskId    int
	TaskType  int // map\reduce
	TaskState int // int\run\done
	NReduce   int // nReduce
	StartTime time.Time

	Input []string
}

const (
	StateMap    = 0
	StateReduce = 1
	StateDone   = 2
)

const (
	TaskStateInit = 0
	TaskStateRun  = 1
	TaskStateDone = 2
)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) WorkerHandler(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.State == StateMap {
		select {
		case reply.Task = <-c.MapTaskChan:
			reply.Task.StartTime = time.Now()
			reply.Task.TaskState = TaskStateRun
		default:
			// log.Println("-------")
			// for _, mapTask := range c.MapTasks {
			// 	log.Println(mapTask.TaskState)
			// }
			// log.Println("-------")
			for _, mapTask := range c.MapTasks {
				if mapTask.TaskState == TaskStateRun && time.Since(mapTask.StartTime) > c.Timeout {
					mapTask.StartTime = time.Now()
					reply.Task = mapTask
					// log.Println("wait")
					// log.Println(mapTask.TaskState)
					// log.Println(mapTask.TaskId)
					return nil
				}
			}
		}
	} else if c.State == StateReduce {
		select {
		case reply.Task = <-c.ReduceTaskChan:
			reply.Task.StartTime = time.Now()
			reply.Task.TaskState = TaskStateRun
		default:
			for _, reduceTask := range c.ReduceTasks {
				if reduceTask.TaskState == TaskStateRun && time.Since(reduceTask.StartTime) > c.Timeout {
					reduceTask.StartTime = time.Now()
					reply.Task = reduceTask
					return nil
				}
			}
		}
	} else if c.State == StateDone {
		reply.WorkerState = WorkerStateDone
	}
	return nil
}

func (c *Coordinator) DoneHandler(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	task := args.Task
	if task.TaskType == TaskTypeMap {
		if task.TaskState == TaskStateRun {
			task.TaskState = TaskStateDone
			c.MapTasks[task.TaskId].TaskState = TaskStateDone
			c.NumDoneMap++
		}
	} else if task.TaskType == TaskTypeReduce {
		if task.TaskState == TaskStateRun {
			task.TaskState = TaskStateDone
			c.ReduceTasks[task.TaskId].TaskState = TaskStateDone
			c.NumDoneReduce++
		}
	}

	if c.State == StateMap {
		if c.NumDoneMap == c.NumMap {
			c.State = StateReduce
			for i := 0; i < c.NumReduce; i++ {
				input := []string{}
				for j := 0; j < c.NumMap; j++ {
					input = append(input, fmt.Sprintf("mr-%d-%d.tmp", j, i))
				}
				task := Task{
					TaskId:    i,
					TaskType:  TaskTypeReduce,
					TaskState: TaskStateInit,
					NReduce:   c.NumReduce,
					StartTime: time.Now(),
					Input:     input,
				}
				c.ReduceTaskChan <- &task
				c.ReduceTasks[i] = &task
			}
		}
	} else if c.State == StateReduce {
		if c.NumDoneReduce == c.NumReduce {
			c.State = StateDone
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret = c.State == StateDone

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
	c.NumMap = len(files)
	c.NumReduce = nReduce
	c.MapTaskChan = make(chan *Task, len(files))
	c.ReduceTaskChan = make(chan *Task, nReduce)
	c.MapTasks = make(map[int]*Task)
	c.ReduceTasks = make(map[int]*Task)
	c.NumDoneMap = 0
	c.NumDoneReduce = 0
	c.State = StateMap
	c.Timeout = time.Duration(time.Second * 10)
	for i, file := range files {
		input := []string{file}
		task := Task{
			TaskId:    i,
			TaskType:  TaskTypeMap,
			TaskState: TaskStateInit,
			Input:     input,
			NReduce:   nReduce,
			StartTime: time.Now(),
		}
		c.MapTaskChan <- &task
		c.MapTasks[i] = &task
	}

	c.server()
	return &c
}
