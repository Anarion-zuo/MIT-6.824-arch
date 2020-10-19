package raft

import (
	"fmt"
	"strconv"
	"sync"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

func (le *LogEntry) Equals(entry *LogEntry) bool {
	return le.Term == entry.Term && le.Command == entry.Command
}

func (le *LogEntry) ToApplyMsg(index int, valid bool) ApplyMsg {
	return ApplyMsg{
		Command:      le.Command,
		CommandValid: valid,
		CommandIndex: index,
	}
}

type RaftLog struct {
	CommitIndex int
	lastApplied int
	entries     []LogEntry
	cond        *sync.Cond
	applyCh     chan ApplyMsg

	raft *Raft
}

func initRaftLogEntries() []LogEntry {
	ret := make([]LogEntry, 1)
	ret[0].Term = -1
	ret[0].Command = nil
	return ret
}

func NewRaftLog(applyCh chan ApplyMsg, raft *Raft) *RaftLog {
	return &RaftLog{
		CommitIndex: 0,
		lastApplied: 0,
		entries:     initRaftLogEntries(),
		cond:        sync.NewCond(&sync.Mutex{}),
		applyCh:     applyCh,
		raft:        raft,
	}
}

var dumpLock sync.Mutex

func (rl *RaftLog) InfoString() string {
	return "commitIndex " + strconv.Itoa(rl.CommitIndex) + " lastApplied " + strconv.Itoa(rl.lastApplied) + " log length " + strconv.Itoa(rl.Length())
}

func (rl *RaftLog) dump() {
	dumpLock.Lock()
	fmt.Println("dumping log", rl.Length())
	fmt.Println("log length", rl.Length(), "commit index", rl.CommitIndex)
	//for entryIndex, entry := range rl.entries {
	//	fmt.Printf("%v term: %v action: %v\n", entryIndex, entry.Term, entry.Command)
	//	if entryIndex == rl.CommitIndex {
	//		fmt.Println("----------------------------------- commit index", entryIndex)
	//	}
	//}
	dumpLock.Unlock()
}

func (rl *RaftLog) Lock() {
	rl.cond.L.Lock()
}

func (rl *RaftLog) Unlock() {
	rl.cond.L.Unlock()
}

func (rl *RaftLog) Append(entries ...LogEntry) {
	rl.entries = append(rl.entries, entries...)
}

// remove all logs starting at this index
func (rl *RaftLog) RemoveAt(index int) {
	rl.entries = rl.entries[:index]
}

func (rl *RaftLog) Index(index int) *LogEntry {
	return &rl.entries[index]
}

func (rl *RaftLog) LastEntry() *LogEntry {
	return &rl.entries[rl.Length()-1]
}

func (rl *RaftLog) Length() int {
	return len(rl.entries)
}

func (rl *RaftLog) firstTermIndex(beginIndex int, term int) int {
	for ; beginIndex > 1; beginIndex-- {
		if rl.entries[beginIndex-1].Term != term {
			return beginIndex
		}
	}
	return 1
}

func (rl *RaftLog) lastTermIndex(beginIndex int, term int) int {
	for ; beginIndex < rl.Length()-1; beginIndex++ {
		if rl.entries[beginIndex].Term == term && rl.entries[beginIndex+1].Term != term {
			return beginIndex + 1
		}
	}
	return -1
}

/*
	From now on, the methods are locked.
	The methods above can compose, by some outer caller, other form of methods, locking taken care of by the outer caller
*/

func (rl *RaftLog) ApplyWorker() {
	for {
		rl.Lock()
		for rl.CommitIndex <= rl.lastApplied {
			rl.cond.Wait()
		}
		rl.lastApplied++
		rl.entries[rl.lastApplied].Apply()
		rl.Unlock()
	}
}

func (rl *RaftLog) UpdateLog(newEntries []LogEntry, prevLogIndex int, leaderCommit int) {
	rl.Lock()

	// update
	for argsEntryIndex := 0; argsEntryIndex < len(newEntries); {
		newEntryIndex := argsEntryIndex + prevLogIndex + 1
		if newEntryIndex < rl.Length() {
			oldEntry := &rl.entries[newEntryIndex]
			newEntry := &newEntries[argsEntryIndex]
			// existing Log
			// check for conflict
			if oldEntry.Equals(newEntry) {
				// consistent!
				rl.raft.printInfo("existing consistent entry", newEntryIndex)
				argsEntryIndex++
			} else {
				// inconsistent!
				// delete everything after current index
				rl.raft.printInfo("inconsistent entry at index", newEntryIndex)
				rl.RemoveAt(newEntryIndex)
			}
		} else {
			// new Log
			// append everything
			rl.raft.printInfo("new entries at", newEntryIndex, "length", len(newEntries)-argsEntryIndex)
			rl.Append(newEntries[argsEntryIndex:]...)
			break
		}
	}
	// commit
	if leaderCommit > rl.CommitIndex {
		oldCommitIndex := rl.CommitIndex
		if leaderCommit < rl.Length()-1 {
			rl.CommitIndex = leaderCommit
		} else {
			rl.CommitIndex = rl.Length() - 1
		}
		for ; oldCommitIndex <= rl.CommitIndex; oldCommitIndex++ {
			if oldCommitIndex == 0 {
				continue
			}
			rl.applyCh <- rl.entries[oldCommitIndex].ToApplyMsg(oldCommitIndex, true)
		}
	}

	rl.Unlock()
}

/*
	Rpc Args
*/
func (rl *RaftLog) NewAppendEntriesArgs(nextIndex int, currentTerm int, me int) *AppendEntriesArgs {
	rl.cond.L.Lock()
	var entries []LogEntry
	prevLogIndex := -1
	prevLogTerm := -1
	if nextIndex >= len(rl.entries) {
		entries = make([]LogEntry, 0)
	} else {
		entries = rl.entries[nextIndex:]
	}
	if nextIndex != 0 {
		prevLogIndex = nextIndex - 1
		prevLogTerm = entries[prevLogIndex].Term
	}
	ret := &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     me,
		LeaderCommit: rl.CommitIndex,
		// index of previous entry of this sending package
		PrevLogIndex: prevLogIndex,
		// term of previous entry of this sending package
		PrevLogTerm: prevLogTerm,
		// sending package
		Entries: entries,
	}
	rl.cond.L.Unlock()
	return ret
}

func (rl *RaftLog) NewRequestVoteArgs(currentTerm int, me int) *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  me,
		LastLogIndex: len(rl.entries) - 1,
		LastLogTerm:  rl.entries[len(rl.entries)-1].Term,
	}
}
