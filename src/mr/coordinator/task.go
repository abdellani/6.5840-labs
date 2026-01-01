package coordinator

const (
	TASK_STATUS_PENDING    = 0
	TASK_STATUS_INPROGRESS = 1
	TASK_STATUS_COMPLETED  = 2
)
const (
	TASK_TYPE_NONE    = 0
	TASK_TYPE_MAPPING = 1
	TASK_TYPE_REDUCE  = 2
)

type Task struct {
	Id     int
	Path   string
	Status int
	Type   int
}

func NewMappingTask(id int, path string) *Task {
	return &Task{
		Id:     id,
		Path:   path,
		Status: TASK_STATUS_PENDING,
		Type:   TASK_TYPE_MAPPING,
	}
}

func (t *Task) IsCompleted() bool {
	return t.Status == TASK_STATUS_COMPLETED
}

func (t *Task) IsInprogress() bool {
	return t.Status == TASK_STATUS_INPROGRESS
}

/*
task can switch between inprogress and pending
but once it's marked as completed, it can't return back to the previous status
*/
func (t *Task) SetInProgress() bool {
	if t.IsCompleted() {
		return false
	}
	t.Status = TASK_STATUS_INPROGRESS
	return true
}

func (t *Task) SetPending() bool {
	if t.IsCompleted() {
		return false
	}
	t.Status = TASK_STATUS_PENDING
	return true
}

/*
When task is pending or in progress, it can move to completed

special case:
worker W1 receive task T1
T1 moves to inprogress
W1 timesout
T1 moves back to inprogress, and scheduled in the queue
W1 sends results lately
T1 will be mark as completed
*/
func (t *Task) SetCompleted() {
	t.Status = TASK_STATUS_COMPLETED
}
