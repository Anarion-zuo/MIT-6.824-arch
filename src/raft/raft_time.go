package raft

import (
	"math/rand"
	"time"
)

type RaftTime struct {
	// waits
	HeartBeatWaitDuration int
	RequestVoteRandMax    int

	// timer
	TimerWaitDuration int
	TimerCleared      bool
}

func (rt *RaftTime) WaitHeartBeat() {
	time.Sleep(time.Millisecond * time.Duration(rt.HeartBeatWaitDuration))
}

func (rt *RaftTime) WaitRandomRequestVote() {
	time.Sleep(time.Duration(rand.Intn(rt.RequestVoteRandMax)))
}

// returns true when expired
func (rt *RaftTime) WaitTimer() bool {
	checkCount := 200
	divDuration := rt.TimerWaitDuration / checkCount
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

func (rt *RaftTime) SetClear() {
	rt.TimerCleared = true
}
