package coordinator

import "log"

type TaskList struct {
	Tasks []*Task
}

func CreateMTasksList(paths []string) *TaskList {
	tasks := []*Task{}
	for i, path := range paths {
		tasks = append(tasks, &Task{
			Id:     i,
			Path:   path,
			Status: TASK_STATUS_PENDING,
			Type:   TASK_TYPE_MAPPING,
		})
	}
	return &TaskList{
		Tasks: tasks,
	}
}

func CreateRTasksList(n int) *TaskList {
	tasks := []*Task{}
	for i := 0; i < n; i++ {
		tasks = append(tasks, &Task{
			Id:     i,
			Path:   "",
			Status: TASK_STATUS_PENDING,
			Type:   TASK_TYPE_REDUCE,
		})
	}
	return &TaskList{
		Tasks: tasks,
	}

}

func (m *TaskList) MarkTaskInprogress(index int) bool {
	task := m.Tasks[index]
	if task.IsCompleted() {
		return false
	}
	task.SetInProgress()
	return true
}
func (m *TaskList) MarkTaskPending(index int) bool {
	task := m.Tasks[index]
	// if the task is  completed, no need to timeout it
	if task.IsCompleted() {
		return false
	}
	task.SetPending()
	return true
}

func (m *TaskList) MarkTaskCompleted(index int) {
	task := m.Tasks[index]
	if task.IsCompleted() {
		log.Println("task already completed")
		return
	}
	task.SetCompleted()

}

func (m TaskList) GetTask(index int) Task {
	return *m.Tasks[index]
}
func (m *TaskList) IsCompleted(index int) bool {
	task := m.Tasks[index]
	return task.IsCompleted()
}
