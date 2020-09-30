package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStat struct {
	beginTime time.Time
	fileName  string
	fileIndex int
	partIndex int
	nReduce   int
	nFiles    int
}

type TaskStatInterface interface {
	GenerateTaskInfo() TaskInfo
	OutOfTime() bool
	GetFileIndex() int
	GetPartIndex() int
	SetNow()
}

type MapTaskStat struct {
	TaskStat
}

type ReduceTaskStat struct {
	TaskStat
}

func (this *MapTaskStat) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		State:     TaskMap,
		FileName:  this.fileName,
		FileIndex: this.fileIndex,
		PartIndex: this.partIndex,
		NReduce:   this.nReduce,
		NFiles:    this.nFiles,
	}
}

func (this *ReduceTaskStat) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		State:     TaskReduce,
		FileName:  this.fileName,
		FileIndex: this.fileIndex,
		PartIndex: this.partIndex,
		NReduce:   this.nReduce,
		NFiles:    this.nFiles,
	}
}

func (this *TaskStat) OutOfTime() bool {
	return time.Now().Sub(this.beginTime) > time.Duration(time.Second*60)
}

func (this *TaskStat) SetNow() {
	this.beginTime = time.Now()
}

func (this *TaskStat) GetFileIndex() int {
	return this.fileIndex
}

func (this *TaskStat) GetPartIndex() int {
	return this.partIndex
}

type TaskStatQueue struct {
	taskArray []TaskStatInterface
	mutex     sync.Mutex
}

func (this *TaskStatQueue) lock() {
	this.mutex.Lock()
}

func (this *TaskStatQueue) unlock() {
	this.mutex.Unlock()
}

func (this *TaskStatQueue) Size() int {
	return len(this.taskArray)
}

func (this *TaskStatQueue) Pop() TaskStatInterface {
	this.lock()
	arrayLength := len(this.taskArray)
	if arrayLength == 0 {
		this.unlock()
		return nil
	}
	ret := this.taskArray[arrayLength-1]
	this.taskArray = this.taskArray[:arrayLength-1]
	this.unlock()
	return ret
}

func (this *TaskStatQueue) Push(taskStat TaskStatInterface) {
	this.lock()
	if taskStat == nil {
		this.unlock()
		return
	}
	this.taskArray = append(this.taskArray, taskStat)
	this.unlock()
}

func (this *TaskStatQueue) TimeOutQueue() []TaskStatInterface {
	outArray := make([]TaskStatInterface, 0)
	this.lock()
	for taskIndex := 0; taskIndex < len(this.taskArray); {
		taskStat := this.taskArray[taskIndex]
		if (taskStat).OutOfTime() {
			outArray = append(outArray, taskStat)
			this.taskArray = append(this.taskArray[:taskIndex], this.taskArray[taskIndex+1:]...)
			// must resume at this index next time
		} else {
			taskIndex++
		}
	}
	this.unlock()
	return outArray
}

func (this *TaskStatQueue) MoveAppend(rhs []TaskStatInterface) {
	this.lock()
	this.taskArray = append(this.taskArray, rhs...)
	rhs = make([]TaskStatInterface, 0)
	this.unlock()
}

func (this *TaskStatQueue) RemoveTask(fileIndex int, partIndex int) {
	this.lock()
	for index := 0; index < len(this.taskArray); {
		task := this.taskArray[index]
		if fileIndex == task.GetFileIndex() && partIndex == task.GetPartIndex() {
			this.taskArray = append(this.taskArray[:index], this.taskArray[index+1:]...)
		} else {
			index++
		}
	}
	this.unlock()
}

type Master struct {
	// Your definitions here.

	filenames []string

	// reduce task queue
	reduceTaskWaiting TaskStatQueue
	reduceTaskRunning TaskStatQueue

	// map task statistics
	mapTaskWaiting TaskStatQueue
	mapTaskRunning TaskStatQueue

	// machine state
	isDone  bool
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.
/*
func (this *Master) TryMap(args *TryMapArgs, reply *TryMapReply) error {
	if this.isMapped {
		reply.RunMap = false
		return nil
	}
	for this.isMapping {
		time.Sleep(time.Duration(1) * time.Second)
	}
	this.isMapped = false
	this.isMapping = true
	reply.RunMap = true
	return nil
}

func (this *Master) MapFinished(args *TryMapArgs, reply *ExampleReply) error {
	this.isMapping = false
	this.isMapped = true
	return nil
}
*/
func (this *Master) AskTask(args *ExampleArgs, reply *TaskInfo) error {
	if this.isDone {
		reply.State = TaskEnd
		return nil
	}

	// check for reduce tasks
	reduceTask := this.reduceTaskWaiting.Pop()
	if reduceTask != nil {
		// an available reduce task
		// record task begin time
		reduceTask.SetNow()
		// note task is running
		this.reduceTaskRunning.Push(reduceTask)
		// setup a reply
		*reply = reduceTask.GenerateTaskInfo()
		fmt.Printf("Distributing reduce task on part %v %vth file %v\n", reply.PartIndex, reply.FileIndex, reply.FileName)
		return nil
	}

	// check for map tasks
	mapTask := this.mapTaskWaiting.Pop()
	if mapTask != nil {
		// an available map task
		// record task begin time
		mapTask.SetNow()
		// note task is running
		this.mapTaskRunning.Push(mapTask)
		// setup a reply
		*reply = mapTask.GenerateTaskInfo()
		fmt.Printf("Distributing map task on %vth file %v\n", reply.FileIndex, reply.FileName)
		return nil
	}

	// all tasks distributed
	if this.mapTaskRunning.Size() > 0 || this.reduceTaskRunning.Size() > 0 {
		// must wait for new tasks
		reply.State = TaskWait
		return nil
	}
	// all tasks complete
	reply.State = TaskEnd
	this.isDone = true
	return nil
}

func (this *Master) distributeReduce() {
	reduceTask := ReduceTaskStat{
		TaskStat{
			fileIndex: 0,
			partIndex: 0,
			nReduce:   this.nReduce,
			nFiles:    len(this.filenames),
		},
	}
	for reduceIndex := 0; reduceIndex < this.nReduce; reduceIndex++ {
		task := reduceTask
		task.partIndex = reduceIndex
		this.reduceTaskWaiting.Push(&task)
	}
}

func (this *Master) TaskDone(args *TaskInfo, reply *ExampleReply) error {
	switch args.State {
	case TaskMap:
		fmt.Printf("Map task on %vth file %v complete\n", args.FileIndex, args.FileName)
		this.mapTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		if this.mapTaskRunning.Size() == 0 && this.mapTaskWaiting.Size() == 0 {
			// all map tasks done
			// can distribute reduce tasks
			this.distributeReduce()
		}
		break
	case TaskReduce:
		fmt.Printf("Reduce task on %vth part complete\n", args.PartIndex)
		this.reduceTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		break
	default:
		panic("Task Done error")
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (this *Master) Done() bool {
	// Your code here.

	return this.isDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// distribute map tasks
	mapArray := make([]TaskStatInterface, 0)
	for fileIndex, filename := range files {
		mapTask := MapTaskStat{
			TaskStat{
				fileName:  filename,
				fileIndex: fileIndex,
				partIndex: 0,
				nReduce:   nReduce,
				nFiles:    len(files),
			},
		}
		mapArray = append(mapArray, &mapTask)
	}
	m := Master{
		mapTaskRunning: TaskStatQueue{taskArray: mapArray},
		nReduce:        nReduce,
		filenames:      files,
	}

	// create tmp directory if not exists
	if _, err := os.Stat("mr-tmp"); os.IsNotExist(err) {
		err = os.Mkdir("mr-tmp", os.ModePerm)
		if err != nil {
			fmt.Print("Create tmp directory failed... Error: %v\n", err)
			panic("Create tmp directory failed...")
		}
	}

	// begin a thread to collect tasks out of time
	go m.collectOutOfTime()

	m.server()
	return &m
}

func (this *Master) collectOutOfTime() {
	for {
		time.Sleep(time.Duration(time.Second * 5))
		timeouts := this.reduceTaskRunning.TimeOutQueue()
		if len(timeouts) > 0 {
			this.reduceTaskWaiting.MoveAppend(timeouts)
		}
		timeouts = this.mapTaskRunning.TimeOutQueue()
		if len(timeouts) > 0 {
			this.mapTaskWaiting.MoveAppend(timeouts)
		}
	}
}
