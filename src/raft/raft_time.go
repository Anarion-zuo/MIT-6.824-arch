package raft

import (
	"math/rand"
	"time"
)

type RaftTimer struct {
	WaitDuration int
	TimerCleared bool
}

func (rt *RaftTimer) SetClear() {
	rt.TimerCleared = true
}

func (rt *RaftTimer) Wait() bool {
	checkCount := 200
	divDuration := rt.WaitDuration / checkCount
	for checkIndex := 0; checkIndex < checkCount; checkIndex++ {
		if rt.TimerCleared {
			// not expired
			rt.TimerCleared = false
			return false
		}
		time.Sleep(time.Millisecond * time.Duration(divDuration))
	}
	ret := !rt.TimerCleared
	rt.TimerCleared = false
	return ret
}

func NewRaftTimer(waitDuration int) RaftTimer {
	return RaftTimer{
		WaitDuration: waitDuration,
		TimerCleared: false,
	}
}

type RaftTime struct {
	// waits
	heartBeatSendWait  int
	requestVoteRandMax int

	// timer
	heartBeatTimer RaftTimer
	//electionTimer RaftTimer
}

func (rt *RaftTime) WaitHeartBeat() {
	time.Sleep(time.Millisecond * time.Duration(rt.heartBeatSendWait))
}

func (rt *RaftTime) WaitRandomRequestVote() {
	time.Sleep(time.Duration(rand.Intn(rt.requestVoteRandMax)))
}

func NewRaftTime(heartBeatSendWait, electionRandMax, heartBeatWaitMax int) *RaftTime {
	return &RaftTime{
		heartBeatSendWait:  heartBeatSendWait,
		requestVoteRandMax: electionRandMax,
		heartBeatTimer:     NewRaftTimer(heartBeatWaitMax),
		//electionTimer:      NewRaftTimer(electionWaitMax),
	}
}
