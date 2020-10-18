package raft

import (
	"labrpc"
	"sync"
)

type AsyncRpcCallInterface interface {
	callback(int)
	tryEnd() bool

	SetAliveHost(int)
	makeRpcCall(peerIndex int) bool
	IncrementCurrentCount()
	PeerCount() int
	shouldExit() bool

	LockCallBack()
	UnlockCallBack()

	GetRaftIndex() int
}

/*
Thus, an async rpc call should only implement these methods:
- makeRpcCall
- callBack
- tryEnd
*/
func CallAsyncRpc(call AsyncRpcCallInterface) {
	for peerIndex := 0; peerIndex < call.PeerCount(); peerIndex++ {
		if call.GetRaftIndex() == peerIndex {
			// don't send to myself
			continue
		}
		go func(peerIndex int) {
			ok := call.makeRpcCall(peerIndex)
			// one callback at a time
			call.LockCallBack()
			call.IncrementCurrentCount()
			if call.shouldExit() {
				call.UnlockCallBack()
				return
			}
			if ok {
				call.SetAliveHost(peerIndex)
				call.callback(peerIndex)
			}
			if call.tryEnd() {
				call.UnlockCallBack()
				return
			}
			call.UnlockCallBack()
		}(peerIndex)
	}
}

type AsyncRpcCallAttr struct {
	// initialized in constructor
	AliveCount   int
	SuccessCount int
	TotalCount   int
	CurrentCount int
	AliveHosts   []bool
	peers        []*labrpc.ClientEnd
	raft         *Raft

	Cond     *sync.Cond
	mu       sync.Mutex
	MustExit bool
}

func (ri *AsyncRpcCallAttr) PeerCount() int {
	return ri.TotalCount
}

func (ri *AsyncRpcCallAttr) IncrementAliveCount() {
	if !ri.MustExit {
		ri.AliveCount++
	}
}

func (ri *AsyncRpcCallAttr) IncrementSuccessCount() {
	if !ri.MustExit {
		ri.SuccessCount++
	}
}

func (ri *AsyncRpcCallAttr) IncrementCurrentCount() {
	if !ri.MustExit {
		ri.CurrentCount++
	}
}

func (ri *AsyncRpcCallAttr) Wait() {
	ri.Cond.L.Lock()
	for ri.CurrentCount < ri.TotalCount && !ri.MustExit {
		ri.Cond.Wait()
	}
	ri.Cond.L.Unlock()
}

func (ri *AsyncRpcCallAttr) SetMustExit() {
	ri.Cond.L.Lock()
	ri.MustExit = true
	ri.Cond.L.Unlock()
	ri.Cond.Broadcast()
}

func (ri *AsyncRpcCallAttr) SetAliveHost(index int) {
	ri.AliveHosts[index] = true
	ri.AliveCount++
}

func (ri *AsyncRpcCallAttr) shouldExit() bool {
	return ri.MustExit
}

func (ri *AsyncRpcCallAttr) LockCallBack() {
	ri.mu.Lock()
}

func (ri *AsyncRpcCallAttr) UnlockCallBack() {
	ri.mu.Unlock()
}

func (ri *AsyncRpcCallAttr) GetRaftIndex() int {
	return ri.raft.me
}

func (rf *Raft) NewAsyncRpcCall() AsyncRpcCallAttr {
	aliveHosts := make([]bool, len(rf.peers))
	for index, _ := range aliveHosts {
		aliveHosts[index] = false
	}
	return AsyncRpcCallAttr{
		TotalCount:   len(rf.peers),
		Cond:         sync.NewCond(&sync.Mutex{}),
		AliveHosts:   aliveHosts,
		AliveCount:   0,
		SuccessCount: 0,
		CurrentCount: 0,
		MustExit:     false,
		peers:        rf.peers,
		raft:         rf,
	}
}
