package mr

import (
	"bufio"
	"encoding/gob"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type workerRecord struct {
	muTime        sync.Mutex
	muTask        sync.Mutex
	taskStartTime time.Time
	currTaskId    int
}

type ReduceTask struct {
	id      int
	content []KeyValue
}

type MapTask struct {
	id       int
	fileName string
	content  string
}

type Coordinator struct {
	// Your definitions here.
	nReduce     int
	nMap        int
	workerLists sync.Map
	startReduce chan bool

	// MapTask
	muMapTask       sync.Mutex
	mapTaskNeedExec int
	mapTaskLists    []*MapTask
	mapTaskQueue    chan *MapTask

	// ReduceTask
	muReduceTask       sync.Mutex
	reduceTaskNeedExec int
	reduceTaskLists    []*ReduceTask
	reduceTaskQueue    chan *ReduceTask
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	workerID := args.WorkerID
	_, exist := c.workerLists.Load(workerID)

	if exist {
		return errors.New(ErrDuplicateWorker)
	}
	reply.ReduceNum = c.nReduce
	worker := workerRecord{currTaskId: -1}
	c.workerLists.Store(workerID, &worker)
	return nil
}

func (c *Coordinator) CompleteReport(args *CompleteReportArgs, reply *CompleteReportReply) error {
	workerID := args.WorkerID
	taskType := args.TaskType
	taskId := args.TaskId
	tempFiles := args.TempFiles
	record, exist := c.workerLists.Load(workerID)

	//fmt.Printf("worker %v complete\n", workerID)

	if !exist {
		return errors.New(ErrNoWorker)
	}

	reply.TaskFinished = false
	record.(*workerRecord).muTask.Lock()
	record.(*workerRecord).currTaskId = -1
	record.(*workerRecord).muTask.Unlock()
	switch taskType {
	case Map:
		c.muMapTask.Lock()
		c.mapTaskNeedExec -= 1
		if c.mapTaskNeedExec == 0 {
			c.startReduce <- true
		}
		c.muMapTask.Unlock()

		for fileId, tempFile := range tempFiles{
			os.Rename(tempFile, "mr-"+strconv.Itoa(taskId)+"-"+strconv.Itoa(fileId)+".txt")
		}
	case Reduce:
		c.muReduceTask.Lock()
		c.reduceTaskNeedExec -= 1
		if c.reduceTaskNeedExec == 0 {
			reply.TaskFinished = true
		}
		c.muReduceTask.Unlock()

		for _, tempFile := range tempFiles{
			os.Rename(tempFile, "mr-out-"+strconv.Itoa(taskId)+".txt")
		}
	}

	return nil
}

func (c *Coordinator) DispatchTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	workerId := args.WorkerID
	record, exist := c.workerLists.Load(workerId)

	if !exist {
		return errors.New(ErrNoWorker)
	}

	record.(*workerRecord).muTime.Lock()
	record.(*workerRecord).taskStartTime = time.Now()
	record.(*workerRecord).muTime.Unlock()

	select {
	case mTask := <-c.mapTaskQueue:
		reply.Content = KeyValue{Key: mTask.fileName, Value: mTask.content}
		reply.TaskType = Map
		reply.TaskId = mTask.id
		record.(*workerRecord).muTask.Lock()
		//fmt.Printf("worker %v get task: %d\n", workerId, mTask.id)
		record.(*workerRecord).currTaskId = mTask.id
		record.(*workerRecord).muTask.Unlock()
	case rTask := <-c.reduceTaskQueue:
		reply.Content = rTask.content
		reply.TaskType = Reduce
		reply.TaskId = rTask.id
		record.(*workerRecord).muTask.Lock()
		record.(*workerRecord).currTaskId = rTask.id
		record.(*workerRecord).muTask.Unlock()
	default:
		reply.TaskType = NoTask
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	gob.Register(KeyValue{})
	gob.Register([]KeyValue{})
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

// start a thread for check alive info
func (c *Coordinator) checkAlive() {
	for {
		t := time.NewTimer(time.Second)
		<-t.C
		c.workerLists.Range(func(k, v interface{}) bool {
			//fmt.Printf("check worker:%v\n", k)
			v.(*workerRecord).muTask.Lock()
			defer v.(*workerRecord).muTask.Unlock()

			if v.(*workerRecord).currTaskId == -1 {
				//fmt.Printf("worker:%v no task\n", k)
				return true
			}

			t := time.Now()
			v.(*workerRecord).muTime.Lock()
			duration := t.Sub(v.(*workerRecord).taskStartTime)
			v.(*workerRecord).muTime.Unlock()

			if duration > (time.Second * AliceCheckDuration) {
				c.workerLists.Delete(k)
				//fmt.Printf("worker %v offline, crash occured\n", k)

				c.muMapTask.Lock()
				if c.mapTaskNeedExec != 0 {
					c.mapTaskQueue <- c.mapTaskLists[v.(*workerRecord).currTaskId]
				}
				c.muMapTask.Unlock()

				c.muReduceTask.Lock()
				if c.reduceTaskNeedExec != 0 {
					c.reduceTaskQueue <- c.reduceTaskLists[v.(*workerRecord).currTaskId]
				}
				c.muReduceTask.Unlock()
			}
			return true
		})
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	c.muReduceTask.Lock()
	if c.reduceTaskNeedExec == 0 {
		return true
	}
	c.muReduceTask.Unlock()

	// Your code here.
	return false
}

// generate map task
func (c *Coordinator) genMapTask(files []string) {
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		value, err := ioutil.ReadAll(f)
		if err != nil {
			log.Fatalf("cannot read %v", file)
		}
		f.Close()

		task := MapTask{fileName: file, content: string(value), id: len(c.mapTaskLists)}
		c.mapTaskLists = append(c.mapTaskLists, &task)
	}

	c.muMapTask.Lock()
	c.mapTaskNeedExec = len(c.mapTaskLists)
	c.muMapTask.Unlock()
	c.nMap = len(c.mapTaskLists)
	for _, t := range c.mapTaskLists {
		c.mapTaskQueue <- t
	}
	fmt.Printf("generate Map Task Complete! MapTaskNumber:%d\n", len(c.mapTaskLists))
}

// generate reduce task
func (c *Coordinator) genReduceTask() {
	for i := 0; i < c.nReduce; i++ {
		kv := []KeyValue{}
		for j := 0; j < c.nMap; j++ {
			file := "mr-" + strconv.Itoa(j) + "-" + strconv.Itoa(i) + ".txt"
			f, err := os.Open(file)
			if err != nil {
				continue
			}
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := scanner.Text()
				result := strings.Fields(line)
				kv = append(kv, KeyValue{Key: result[0], Value: result[1]})
			}
			f.Close()
		}

		task := ReduceTask{id: len(c.reduceTaskLists), content: kv}
		c.reduceTaskLists = append(c.reduceTaskLists, &task)
	}

	c.muReduceTask.Lock()
	c.reduceTaskNeedExec = len(c.reduceTaskLists)
	c.muReduceTask.Unlock()
	for _, t := range c.reduceTaskLists {
		c.reduceTaskQueue <- t
	}
	fmt.Printf("generate Reduce Task Complete! ReduceTaskNumber:%d\n", len(c.reduceTaskLists))
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{mapTaskQueue: make(chan *MapTask, MaxTaskNumber), reduceTaskQueue: make(chan *ReduceTask,
		MaxTaskNumber), startReduce: make(chan bool)}
	// Your code here.
	c.nReduce = nReduce
	c.server()
	c.genMapTask(files)
	go c.checkAlive()

	// start reduce task
	<-c.startReduce
	time.Sleep(2)
	c.genReduceTask()

	return &c
}
