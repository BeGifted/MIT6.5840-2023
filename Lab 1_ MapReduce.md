# 前置知识
MapReduce：Master 将一个 Map 任务或 Reduce 任务分配给一个空闲的 worker。
**Map阶段**：被分配了 map 任务的 worker 程序读取相关的输入数据片段，生成并输出中间 k/v 对，并缓存在内存中。
**Reduce阶段**：所有 map 任务结束，reduce 程序使用 RPC 从 map worker 所在主机的磁盘上读取缓存数据，通过对 key 进行排序后使得具有相同 key 值的数据聚合在一起，reduce 进行操作后输出为文件。
![image.png](https://cdn.nlark.com/yuque/0/2023/png/39241706/1701618313610-58ee93d3-bba6-46db-8756-3d7a6ff84886.png#averageHue=%23f9f9f8&clientId=uf3eefb14-0032-4&from=paste&height=594&id=u81b11f4b&originHeight=594&originWidth=893&originalType=binary&ratio=1&rotation=0&showTitle=false&size=88302&status=done&style=none&taskId=u61cc1fea-b2c0-45c6-a7e9-d3ac03372e3&title=&width=893)
# 实验内容
实现一个分布式 MapReduce，由两个程序（coordinator 和 worker）组成。只有一个 coordinator 和一个或多个并行执行的 worker 。在真实系统中， worker 会运行在多台不同的机器上，但在本 lab 中将在一台机器上运行所有 worker 。 worker 将通过 RPC 与 coordinator 通话。每个 worker 进程都会向 coordinator 请求 task，从一个或多个文件中读取 task 输入，执行 task，并将 task 输出写入一个或多个文件。 coordinator 应该注意到，如果某个 worker 在合理的时间内（本 lab 使用 10 秒）没有完成任务，就会将相同的 task 交给另一个 worker。
rpc举例：[https://pdos.csail.mit.edu/6.824/notes/kv.go](https://pdos.csail.mit.edu/6.824/notes/kv.go)
lab内容：[https://pdos.csail.mit.edu/6.824/labs/lab-mr.html](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
Impl：mr/coordinator.go、mr/worker.go、mr/rpc.go
总结一下
对于 Coordiantor：

- map 任务初始化；
- rpc handler：回应worker分配任务请求、回应worker任务完成通知；
- 自身状态控制，处理 map/reduce 阶段，还是已经全部完成；
- 任务超时重新分配；

对于 Worker：

- 给 coordinator 发送 rpc 请求分配任务；
- 给 coordinator 发送 rpc 通知任务完成；
- 自身状态控制，准确来说是 coordinator 不需要 worker 工作时，通知 worker 结束运行；
# 实验环境
OS：WSL-Ubuntu-18.04

golang：go1.17.6 linux/amd64
# 概要设计
所设计的Coordinator、Task、rpc消息格式：
```go
type WorkerArgs struct {
	WorkerId    int
	WorkerState int // init\done\fail
	Task        *Task
}

type WorkerReply struct {
	WorkerId    int
	WorkerState int // init\done\fail
	Task        *Task
}

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

const (
	TaskTypeMap    = 0
	TaskTypeReduce = 1
)

const (
	WorkerStateInit = 0
	WorkerStateDone = 1
	WorkerStateFail = 2
)
```
（TODO）WorkerState 出现 fail 的原因主要是在文件无法打开或读取上，如果是在处理 map 任务时出现 fail，那只有可能是原文件丢失了；如果是 reduce 任务时出现 fail，表示中间文件丢失，需要运行某个特定的 map 任务重新生成，然后再重新开始该 reduce 任务。当然，不实现这个也不会影响 test。
# 主要流程
## 创建 Coordinator
创建 Coordinator 并且初始化，将需要处理的数据片段放入 MapTaskChan 信道。这里将单个文件视作一个数据片段进行处理，也就是说有 len(files) 个 map 任务。
```go
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
```
## 运行 Worker
Worker 主要处理两类任务：map 和 reduce。这两类任务通过 rpc 与 Coordinator 通信获取。
map 任务处理：
```go
if task.TaskType == TaskTypeMap {
    filename := task.Input[0]
    intermediate := []KeyValue{}
    file, err := os.Open(filename)
    if err != nil {
        log.Fatalf("cannot open %v", filename)
        continue
    }
    content, err := ioutil.ReadAll(file)
    if err != nil {
        log.Fatalf("cannot read %v", filename)
        continue
    }
    file.Close()
    // log.Println("mapf")
    // log.Println(task.TaskId)
    kva := mapf(filename, string(content))
    intermediate = append(intermediate, kva...)

    // sort.Sort(ByKey(intermediate))

    ReduceSplit := make(map[int][]KeyValue)
    for _, kv := range intermediate {
        ReduceSplit[ihash(kv.Key)%task.NReduce] = append(ReduceSplit[ihash(kv.Key)%task.NReduce], kv)
    }

    for i := 0; i < task.NReduce; i++ {
        oname := fmt.Sprintf("mr-%d-%d.tmp", task.TaskId, i)
        ofile, _ := os.Create(oname)
        enc := json.NewEncoder(ofile)
        for _, kv := range ReduceSplit[i] {
            err := enc.Encode(&kv)
            if err != nil {
                log.Fatalf("cannot encode %v", kv)
                break
            }
        }
        ofile.Close()
    }

    // Task Done
    args.Task = task
    TaskDone(&args)
}
```
reduce 任务处理：
```go
if task.TaskType == TaskTypeReduce {
    var kva ByKey
    for _, filename := range task.Input {
        file, err := os.Open(filename)
        if err != nil {
            log.Fatalf("cannot open %v", filename)
            file.Close()
            continue
        }

        dec := json.NewDecoder(file)
        for {
            var kv KeyValue
            if err := dec.Decode(&kv); err != nil {
                break
            }
            kva = append(kva, kv)
        }
        file.Close()
    }

    sort.Sort(kva)

    i := 0
    oname := fmt.Sprintf("mr-out-%d", task.TaskId)
    ofile, _ := os.Create(oname)
    for i < len(kva) {
        j := i + 1
        for j < len(kva) && kva[j].Key == kva[i].Key {
            j++
        }
        values := []string{}
        for k := i; k < j; k++ {
            values = append(values, kva[k].Value)
        }
        output := reducef(kva[i].Key, values)

        fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
        i = j
    }

    // Task Done
    args.Task = task
    TaskDone(&args)
}
```
## WorkerHandler
给 worker 分配 map/reduce 任务，取决于阶段任务是否全部完成，当阶段任务全部完成，coordinator 的状态也需要更新。这个过程全局加锁。
处理 map 阶段：
```go
if c.State == StateMap {
    select {
    case reply.Task = <-c.MapTaskChan:
        reply.Task.StartTime = time.Now()
        reply.Task.TaskState = TaskStateRun
    default:
        for _, mapTask := range c.MapTasks {
            if mapTask.TaskState == TaskStateRun && time.Since(mapTask.StartTime) > c.Timeout {
                mapTask.StartTime = time.Now()
                reply.Task = mapTask
                return nil
            }
        }
    }
}
```
处理 reduce 阶段：
```go
if c.State == StateReduce {
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
}
```
需要注意的是，除了这两个阶段外还有 StateDone 阶段，即 reduce 任务都执行完毕了，coordinator 还没完全回收，此时 worker 还在请求分配任务，这时候就应该通知 worker 停止。
## DoneHandler
coordinator 处理任务完成的通知。全程加锁。在这里更新 task/coordinator 状态。
```go
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
```
# 实验结果
```shell
bash test-mr-many.sh 10
```
![image.png](https://cdn.nlark.com/yuque/0/2023/png/39241706/1701857784684-545efb2d-6617-48db-a437-924821bd6470.png#averageHue=%23272523&clientId=uf3eefb14-0032-4&from=paste&height=394&id=u9521ebd3&originHeight=394&originWidth=662&originalType=binary&ratio=1&rotation=0&showTitle=false&size=36502&status=done&style=none&taskId=uab471698-1f95-40fb-bad9-d46cc5a40b1&title=&width=662)