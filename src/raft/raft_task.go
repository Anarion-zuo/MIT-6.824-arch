package raft

import "sync"

/*
A Task class should implement all methods in RaftTask interface,
and inherits struct RaftTaskAttr
*/

type RaftTask interface {
	execute()

	WaitForDone()
	SetDone()
}

// rpc calls this and wait for return
func RunTask(rt RaftTask, queue *RaftTaskQueue) {
	queue.Push(rt)
	rt.WaitForDone()
}

type RaftTaskAttr struct {
	done     bool
	doneCond *sync.Cond

	raft *Raft
}

func NewRaftTaskAttr(raft *Raft) RaftTaskAttr {
	return RaftTaskAttr{
		done:     false,
		doneCond: sync.NewCond(&sync.Mutex{}),

		raft: raft,
	}
}

func (rtd *RaftTaskAttr) WaitForDone() {
	rtd.doneCond.L.Lock()
	for rtd.done == false {
		rtd.doneCond.Wait()
	}
	rtd.doneCond.L.Unlock()
}

func (rtd *RaftTaskAttr) SetDone() {
	rtd.doneCond.L.Lock()
	if rtd.done {
		panic("Task done twice...")
	}
	rtd.done = true
	rtd.doneCond.L.Unlock()
	rtd.doneCond.Broadcast()
}

type RaftTaskQueue struct {
	channel chan RaftTask
}

func NewRaftTaskQueue() *RaftTaskQueue {
	return &RaftTaskQueue{
		channel: make(chan RaftTask),
	}
}

func (rtq *RaftTaskQueue) pop() RaftTask {
	return <-rtq.channel
}

func (rtq *RaftTaskQueue) RunOne() {
	task := rtq.pop()
	task.execute()
	task.SetDone()
}

func (rtq *RaftTaskQueue) Push(rt RaftTask) {
	rtq.channel <- rt
}
