package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// error enum
const (
	OK                 = "ok"
	ErrDuplicateWorker = "duplicate worker"
	ErrNoWorker = "worker not registered"
)

// task enum
const (
	Map = iota
	Reduce
	NoTask
)

const AliceCheckDuration = 10
const MaxTaskNumber = 100

type RequestTaskArgs struct {
	WorkerID string
}

type RequestTaskReply struct{
	Content interface{}
	TaskType int
	TaskId int
}

type CompleteReportArgs struct {
	WorkerID string
	TempFiles []string
	TaskId int
	TaskType int
}

type CompleteReportReply struct{
	TaskFinished bool
}

type RegisterArgs struct {
	WorkerID string
}
type RegisterReply struct{
	ReduceNum int
}

type AliveReportArgs struct {
	WorkerID  string
	CheckTime time.Time
}
type AliveReportReply struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
