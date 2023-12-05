package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		args := WorkerArgs{
			WorkerState: WorkerStateInit,
		}
		reply := WorkerReply{}
		ok := call("Coordinator.WorkerHandler", &args, &reply)
		if ok {
			if reply.WorkerState == WorkerStateDone {
				break
			}

			task := reply.Task

			// wait for new task
			if task == nil {
				continue
			}

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
			} else if task.TaskType == TaskTypeReduce {
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
		} else {
			log.Println("call fail")
			break
		}
	}
}

func TaskDone(args *WorkerArgs) bool {
	args.WorkerState = WorkerStateDone
	reply := WorkerReply{}
	ok := call("Coordinator.DoneHandler", &args, &reply)
	return ok
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
