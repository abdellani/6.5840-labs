package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task, err := GetTask()
		if err != nil {
			break
		}
		switch task.Type {
		case TASK_TYPE_NONE:
			// log.Println("waiting ...")
			time.Sleep(3 * time.Second)
		case TASK_TYPE_REDUCE:
			HandleReduceTask(reducef, task)
		case TASK_TYPE_MAPPING:
			HandleMapTask(mapf, task)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func HandleReduceTask(reducef func(string, []string) string, task *TaskRPCReply) {
	// log.Printf("Reduce Task : %d\n", task.Id)
	pattern := fmt.Sprintf("./mr-*-%d", task.Id)
	files, _ := filepath.Glob(pattern)
	content := []KeyValue{}
	for _, filename := range files {
		file, _ := os.Open(filename)
		decoder := json.NewDecoder(file)
		fileContent := []KeyValue{}
		decoder.Decode(&fileContent)
		for i := 0; i < len(fileContent); i++ {
			content = append(content, fileContent[i])
		}

	}
	sort.Sort(ByKey(content))
	file, _ := os.CreateTemp(".", "tmp-*.txt")
	i := 0
	for i < len(content) {
		j := i + 1
		for j < len(content) && content[j].Key == content[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, content[k].Value)
		}
		output := reducef(content[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", content[i].Key, output)

		i = j
	}
	outputFilename := fmt.Sprintf("mr-out-%d", task.Id)
	os.Rename(file.Name(), outputFilename)
	NotifyTaskCompletion(task.Id, task.Type)
}
func HandleMapTask(mapf func(string, string) []KeyValue, task *TaskRPCReply) {
	// log.Printf("Mapping Task : %d\n", task.Id)
	content := loadFileContent(task.Path)
	mapped := mapf(task.Path, content)
	grouped := GroupKeys(mapped, task.R)
	SaveGroupedKeys(task.Id, grouped)
	NotifyTaskCompletion(task.Id, task.Type)

}
func NotifyTaskCompletion(taskId int, taskType int) {
	arg := TaskCompletionNotificationArg{Id: taskId, Type: taskType}
	reply := EmptyReply{}
	call("Coordinator.MarkTaskCompleted", &arg, &reply)
}

func GetTask() (*TaskRPCReply, error) {
	args := EmptyArgs{}
	reply := TaskRPCReply{}
	done := call("Coordinator.GetTask", &args, &reply)
	if done == false {
		return nil, errors.New("call failed")
	}
	return &reply, nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
func loadFileContent(path string) string {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("can open %v", path)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("can read content of %v", file)
	}
	return string(content)
}

func GroupKeys(data []KeyValue, r int) [][]KeyValue {
	result := [][]KeyValue{}
	for i := 0; i < 10; i++ {
		result = append(result, []KeyValue{})
	}
	for _, item := range data {
		hash := ihash(item.Key)
		index := hash % r
		result[index] = append(result[index], item)
	}
	return result
}

func SaveGroupedKeys(taskId int, data [][]KeyValue) {
	for idx, array := range data {
		if len(array) == 0 {
			continue
		}
		file, err := os.CreateTemp(".", "tmp-*.txt")
		if err != nil {
			log.Fatal("cannot create tmp file to store results")
		}
		encoder := json.NewEncoder(file)
		encoder.Encode(array)
		permanentFilename := fmt.Sprintf("mr-%d-%d", taskId, idx)
		os.Rename(file.Name(), permanentFilename)
	}
}
