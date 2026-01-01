package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"6.5840/mr/coordinator"
)

/*
the coordinator is reponsible for
* prepare a list of mapping tasks
* assing the mapping tasks to the workers
* After assigning a tasks, check if the task is achieved or not
	* if the task is achieved, update the count
	*	if not, assign the task to another worker, and update the count
* once all the tasks are done, start assigning the reduce tasks
*/

/*
  mapping tasks list
	Mutex for access the mapping tasks
	timeout checking gorouting
	will use a stack that stores the pending mapping tasks
	the tasks will have three stats
		* pending, inprogress, completed

	the mapping task will have
		* index
		* status
		* filename

	when a worker ask for a task, the coordinator will
		* check the count of completed tasks, if all mapping tasks, it'll assign a reduce task
		* if all tasks are completed, close
		* check the queue of pending tasks
		*	pop one item
		*  update the tasks status to in progress
		* launch the timeout

*/

type Coordinator struct {
	// Your definitions here.
	MTasks coordinator.TaskManager
	RTasks coordinator.TaskManager
	R      int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) GetTask(args *EmptyArgs, reply *TaskRPCReply) error {
	var task *coordinator.Task
	var err error
	if c.MTasks.IsDone() {
		task, err = c.RTasks.GetNextTask()
	} else {
		task, err = c.MTasks.GetNextTask()
	}
	if task == nil {
		// all tasks are assigned, but not all of them are done
		task = &coordinator.Task{
			Type: TASK_TYPE_NONE,
		}
	}
	// TODO: case when error is not null
	if task == nil && err != nil {
		return err
	}
	reply.Id = task.Id
	reply.Path = task.Path
	reply.Status = task.Status
	reply.Type = task.Type
	reply.R = c.R
	return nil
}

func (c *Coordinator) MarkTaskCompleted(args *TaskCompletionNotificationArg, reply *EmptyReply) error {
	switch args.Type {
	case TASK_TYPE_MAPPING:
		c.MTasks.MarkCompleted(args.Id)
	case TASK_TYPE_REDUCE:
		c.RTasks.MarkCompleted(args.Id)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	c.MTasks.Serve()
	c.RTasks.Serve()

	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	log.Println("Listening on ", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.MTasks.IsDone() && c.RTasks.IsDone()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MTasks: *coordinator.CreateMappingTaskManager(files),
		RTasks: *coordinator.CreateReduceTaskManager(nReduce),
		R:      nReduce,
	}

	// Your code here.

	c.server()
	return &c
}
