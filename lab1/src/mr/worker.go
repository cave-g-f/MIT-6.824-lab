package mr

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueSlice []KeyValue

func (kv KeyValueSlice) Len() int {
	return len(kv)
}
func (kv KeyValueSlice) Less(i, j int) bool {
	return kv[i].Key < kv[j].Key
}
func (kv KeyValueSlice) Swap(i, j int) {
	kv[i], kv[j] = kv[j], kv[i]
}

type worker struct {
	id       string
	nReduce  int
	needExit chan bool
}

//
// main/mrworker.go calls this function.
//
func MakeWorker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	w := worker{needExit: make(chan bool)}
	w.register()
	gob.Register(KeyValue{})
	gob.Register([]KeyValue{})
	go w.requestAndReportTask(mapf, reducef)
	<-w.needExit
}

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
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (w *worker) register() {
	w.id = strconv.Itoa(os.Getpid())
	reply := RegisterReply{}
	args := RegisterArgs{WorkerID: w.id}
	call("Coordinator.Register", &args, &reply)
	w.nReduce = reply.ReduceNum
}

func (w *worker) requestAndReportTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		reply := RequestTaskReply{}
		call("Coordinator.DispatchTask", &RequestTaskArgs{w.id}, &reply)

		taskType := reply.TaskType
		taskId := reply.TaskId

		reportArgs := CompleteReportArgs{TaskId: taskId, WorkerID: w.id, TaskType: taskType}
		reportReply := CompleteReportReply{}

		switch taskType {
		case Map:
			res := mapf(reply.Content.(KeyValue).Key, reply.Content.(KeyValue).Value)

			var tempFiles []string
			for i := 0; i < w.nReduce; i++{
				tempf, err := ioutil.TempFile("./", "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(i))
				if err != nil{
					log.Fatal(err)
				}
				tempFiles = append(tempFiles, tempf.Name())
				tempf.Close()
			}

			for _, kv := range res {
				fileId := ihash(kv.Key) % w.nReduce
				outputFile(tempFiles[fileId], []KeyValue{kv})
			}

			reportArgs.TempFiles = tempFiles
			call("Coordinator.CompleteReport", &reportArgs, &reportReply)
		case Reduce:
			kv := KeyValueSlice(reply.Content.([]KeyValue))
			sort.Sort(kv)
			result := []KeyValue{}
			for i := 0; i < len(kv); {
				j := i + 1
				for j < len(kv) && kv[j].Key == kv[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kv[k].Value)
				}
				output := reducef(kv[i].Key, values)
				result = append(result, KeyValue{Key: kv[i].Key, Value: output})
				i = j
			}

			var tempFiles []string
			tempf, err := ioutil.TempFile("./", "mr-" + strconv.Itoa(taskId))
			if err != nil{
				log.Fatal(err)
			}
			tempFiles = append(tempFiles, tempf.Name())
			tempf.Close()

			outputFile(tempFiles[0], result)
			reportArgs.TempFiles = tempFiles

			call("Coordinator.CompleteReport", &reportArgs, &reportReply)
			if reportReply.TaskFinished {
				w.needExit <- true
				os.Exit(1)
			}
		case NoTask:
			time.Sleep(time.Second)
		}
	}
}

func outputFile(filename string, content []KeyValue){
	outfile, _ := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND, 0666)

	for _, kv := range content{
		fmt.Fprintf(outfile, "%v %v\n", kv.Key, kv.Value)
	}

	outfile.Close()
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		log.Fatal("crash:", err)
	}

	return false
}
