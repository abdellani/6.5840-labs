package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"

	"6.5840/mr/coordinator"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type EmptyArgs struct {
}
type EmptyReply struct {
}

// Add your RPC definitions here.
const (
	TASK_TYPE_NONE    = 0
	TASK_TYPE_MAPPING = 1
	TASK_TYPE_REDUCE  = 2
)

type TaskRPCReply struct {
	coordinator.Task
	R int
}

type TaskCompletionNotificationArg struct {
	Id   int
	Type int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
