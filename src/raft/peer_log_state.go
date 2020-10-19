package raft

import "sync"

type PeerLogStates struct {
	NextIndex  []int
	matchIndex []int // init 0

	mutex sync.Mutex
}

func initPeerCountIntArray(peerCount int, initVal int) []int {
	ret := make([]int, peerCount)
	for index := 0; index < peerCount; index++ {
		ret[index] = initVal
	}
	return ret
}

func NewPeerLogStates(peerCount int) PeerLogStates {
	return PeerLogStates{
		NextIndex:  initPeerCountIntArray(peerCount, 0),
		matchIndex: initPeerCountIntArray(peerCount, 0),
	}
}

func (pls *PeerLogStates) Lock() {
	pls.mutex.Lock()
}

func (pls *PeerLogStates) Unlock() {
	pls.mutex.Unlock()
}

func (pls *PeerLogStates) SetAllNextIndex(nextIndex int) {
	pls.mutex.Lock()
	for index := 0; index < len(pls.NextIndex); index++ {
		pls.NextIndex[index] = nextIndex
	}
	pls.mutex.Unlock()
}

func (pls *PeerLogStates) More(peerIndex int, moreNextIndex int) {
	pls.mutex.Lock()
	pls.NextIndex[peerIndex] += moreNextIndex
	pls.matchIndex[peerIndex] = pls.NextIndex[peerIndex] - 1
	pls.mutex.Unlock()
}

func (pls *PeerLogStates) Less(peerIndex int) {
	pls.mutex.Lock()
	pls.NextIndex[peerIndex] /= 2
	pls.mutex.Unlock()
}
