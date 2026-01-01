package coordinator

import (
	"log"
	"sync"
	"time"
)

// one manager for mapping tasks
// one manager for reducer
type TaskManager struct {
	Tasks           TaskList
	Pending         Queue
	Completed       int
	Type            int
	TimeoutDuration int
	TimeoutIdsChan  chan int
	Lock            sync.Mutex
}

func CreateMappingTaskManager(files []string) *TaskManager {
	return &TaskManager{
		Tasks:           *CreateMTasksList(files),
		Pending:         *NewTaskQueue(len(files)),
		Completed:       0,
		TimeoutDuration: 10,
		TimeoutIdsChan:  make(chan int),
	}
}

func CreateReduceTaskManager(n int) *TaskManager {
	return &TaskManager{
		Tasks:           *CreateRTasksList(n),
		Pending:         *NewTaskQueue(n),
		Completed:       0,
		TimeoutDuration: 10,
		TimeoutIdsChan:  make(chan int),
	}

}
func (t *TaskManager) Serve() {
	t.serve()
}
func (t *TaskManager) serve() {
	go t.TimeoutMonitoringLoop()
}

func (t *TaskManager) TimeoutMonitoringLoop() {
	for {
		id := <-t.TimeoutIdsChan
		// log.Println("received signal to reschedule task")
		go t.MoveToPending(id)
	}
}

func (t *TaskManager) MoveToPending(taskID int) {
	t.Lock.Lock()
	defer t.Lock.Unlock()
	if t.Tasks.IsCompleted(taskID) {
		// log.Printf("Task %d already completed\n", taskID)
		return
	}
	t.Tasks.MarkTaskInprogress(taskID)
	t.Pending.Push(taskID)
	// log.Printf("task %d  rescheduled\n", taskID)
}

func (t *TaskManager) IsDone() bool {
	t.Lock.Lock()
	defer t.Lock.Unlock()
	return len(t.Tasks.Tasks) == t.Completed
}

func (t *TaskManager) ScheduleTimeoutSignal(id int) {
	<-time.After(time.Duration(t.TimeoutDuration) * time.Second)
	log.Printf("Sending signal to reschedule the task")
	t.TimeoutIdsChan <- id
	log.Println("signal sent!")
}

func (t *TaskManager) GetNextTask() (*Task, error) {
	t.Lock.Lock()
	defer t.Lock.Unlock()

	taskId, err := t.Pending.Pop()
	if err != nil {
		return nil, err
	}

	t.Tasks.MarkTaskInprogress(taskId)
	go t.ScheduleTimeoutSignal(taskId)
	task := t.Tasks.GetTask(taskId)
	return &task, nil
}

func (t *TaskManager) MarkCompleted(id int) {
	t.Lock.Lock()
	defer t.Lock.Unlock()
	// log.Printf("marking task completed %d \n", id)
	if t.Tasks.IsCompleted(id) {
		return
	}
	t.Tasks.MarkTaskCompleted(id)
	t.Completed++
}
