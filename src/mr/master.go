package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.

	// map state
	isMapped  bool
	isMapping bool
}

// Your code here -- RPC handlers for the worker to call.

func (this *Master) initMaster() {
	this.isMapped = false
	this.isMapping = false
}

func (this *Master) TryMap(args *ExampleArgs, reply *TryMapReply) error {
	if this.isMapped {
		reply.runMap = false
		return nil
	}
	for this.isMapping {
		time.Sleep(time.Duration(1) * time.Second)
	}
	this.isMapped = false
	this.isMapping = true
	reply.runMap = true
	return nil
}

func (this *Master) MapFinished(args *ExampleArgs, reply *ExampleReply) error {
	this.isMapping = false
	this.isMapped = true
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
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.server()
	return &m
}
