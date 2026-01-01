package coordinator

import (
	"errors"
)

type Queue struct {
	Ids       []int
	SizeLimit int
	Top       int
}

func NewTaskQueue(sizeLimit int) *Queue {
	ids := []int{}
	for i := 0; i < sizeLimit; i++ {
		ids = append(ids, i)
	}
	return &Queue{
		Ids:       ids,
		Top:       sizeLimit,
		SizeLimit: sizeLimit,
	}
}

func (q *Queue) Push(id int) error {
	if q.Top >= q.SizeLimit {
		return errors.New("reached Queues size")
	}
	q.Ids[q.Top] = id
	q.Top++
	return nil
}
func (q *Queue) Pop() (int, error) {
	if q.Top == 0 {
		return -1, errors.New("queue is empty")
	}
	q.Top--
	return q.Ids[q.Top], nil
}

func (q *Queue) IsEmpty() bool {
	return q.Top == 0
}

func (q *Queue) IsFull() bool {
	return q.Top == q.SizeLimit
}

func (q *Queue) ItemsCount() int {
	return q.Top
}
