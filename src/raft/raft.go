package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"strconv"
	"sync"
)
import "sync/atomic"
import "labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

func (le *LogEntry) Apply() {

}

//
// A Go object implementing a single Raft peer.
//
const (
	LeaderState    = 0
	FollowerState  = 1
	CandidateState = 2
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int

	taskQueue *RaftTaskQueue

	MyState RaftState
	Log     *RaftLog

	// other peer states
	peerLogStates PeerLogStates

	// wait duration parameters in ms
	TimeParams *RaftTime
}

func NewRaft(applyCh chan ApplyMsg, peers []*labrpc.ClientEnd, persister *Persister, me int, timeParams *RaftTime) *Raft {
	peerCount := len(peers)
	ret := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		//dead:

		currentTerm: 0,
		votedFor:    -1, // voted for no one
		taskQueue:   NewRaftTaskQueue(),

		peerLogStates: NewPeerLogStates(peerCount),
		TimeParams:    timeParams,
	}
	ret.MyState = NewFollowerState(ret)
	ret.Log = NewRaftLog(applyCh, ret)
	return ret
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).

	return rf.currentTerm, rf.MyState.IsLeader()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

/*
	ToString info
*/
func (rf *Raft) PrefixString() string {
	return "[" + "id " + strconv.Itoa(rf.me) + " " + rf.MyState.ToString() + " term " + strconv.Itoa(rf.currentTerm) + " votedFor " + strconv.Itoa(rf.votedFor) + " " + rf.Log.InfoString() + "]"
}

func (rf *Raft) printInfo(strings ...interface{}) {
	fmt.Println(rf.PrefixString(), strings)
}

func (rf *Raft) PeerCount() int {
	return len(rf.peers)
}

func (rf *Raft) tryFollowNewerTerm(candidateId int, newTerm int) bool {
	if rf.currentTerm < newTerm {
		//fmt.Println(rf.PrefixPrint(), "sees newer term", newTerm, "from peer", candidateId)
		rf.printInfo("sees a newer term", newTerm)
		rf.currentTerm = newTerm
		rf.toFollower(candidateId)
		return true
	}
	return false
}

func (rf *Raft) tryDiscardOldTerm(peerId int, oldTerm int) bool {
	if rf.currentTerm > oldTerm {
		//fmt.Println(rf.PrefixPrint(), "received args from peer", peerId, " of term", oldTerm, ", discarding")
		return true
	}
	return false
}

func (rf *Raft) TryCommit(call *AppendEntriesCall) {
	rf.Log.Lock()
	rf.peerLogStates.Lock()
	oldCommit := rf.Log.CommitIndex
	rf.printInfo("trying to commit new entries")
	for {
		testCommitIndex := rf.Log.CommitIndex + 1
		if testCommitIndex >= rf.Log.Length() {
			rf.printInfo("all present entries committed")
			break
		}
		if call.AliveCount <= call.TotalCount/2 {
			rf.printInfo("#alive", call.AliveCount, "too few for #total", call.TotalCount, "to reach an agreement")
			break
		}
		rf.printInfo("#alive", call.AliveCount, "sufficient for #total", call.TotalCount, "to reach an agreement")
		trueCount := 1
		for peerIndex, matchIndex := range rf.peerLogStates.matchIndex {
			if peerIndex == rf.me {
				continue
			}
			if call.AliveHosts[peerIndex] {
				// this host is alive
				if matchIndex >= testCommitIndex {
					trueCount++
				}
			}
		}
		if trueCount <= call.TotalCount/2 {
			rf.printInfo("#consistent", trueCount, "too few for #alive", call.AliveCount)
			break
		}
		rf.printInfo("#consistent", trueCount, "sufficient for #alive", call.AliveCount)
		if rf.Log.Index(testCommitIndex).Term != rf.currentTerm {
			// discard uncommitted logs
			rf.Log.RemoveAt(testCommitIndex)
			// reset NextIndex
			for peerIndex := 0; peerIndex < rf.PeerCount(); peerIndex++ {
				if rf.peerLogStates.NextIndex[peerIndex] >= testCommitIndex {
					rf.peerLogStates.NextIndex[peerIndex] = rf.Log.CommitIndex
				}
			}
			rf.printInfo("discards inconsistent term entries starting", testCommitIndex)
			break
		}
		rf.Log.CommitIndex = testCommitIndex
		//rf.Log.applyCh <- rf.Log.Index(rf.Log.CommitIndex).ToApplyMsg(rf.Log.CommitIndex, true)
		rf.printInfo("new entry committed command", rf.Log.Index(rf.Log.CommitIndex).Command)
	}
	if rf.Log.CommitIndex > oldCommit {
		for ; oldCommit <= rf.Log.CommitIndex; oldCommit++ {
			rf.Log.applyCh <- rf.Log.Index(oldCommit).ToApplyMsg(oldCommit, true)
		}
	}
	rf.peerLogStates.Unlock()
	rf.Log.Unlock()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	RunTask(NewRequestVoteTask(rf, args, reply), rf.taskQueue)
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	RunTask(NewAppendEntriesTask(rf, args, reply), rf.taskQueue)
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).

	rf.LockPeerState()
	if (!rf.MyState.IsLeader()) || rf.killed() {
		rf.UnlockPeerState()
		return -1, -1, false
	}

	rf.Log.Lock()
	index := rf.Log.Length()
	rf.Log.Append(LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
	rf.Log.Unlock()

	rf.peerLogStates.Lock()
	for peerIndex := 0; peerIndex < rf.PeerCount(); peerIndex++ {
		rf.peerLogStates.NextIndex[peerIndex] = index
	}
	rf.peerLogStates.Unlock()

	term := rf.currentTerm
	isLeader := rf.MyState.IsLeader()

	rf.UnlockPeerState()
	rf.Log.cond.Broadcast()

	rf.printInfo("got 1 new command", command)

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.printInfo("killed by outer caller")
	rf.Log.dump()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) newAppendEntriesArgs(peerIndex int) *AppendEntriesArgs {
	rf.Log.Lock()

	thisNextIndex := rf.peerLogStates.NextIndex[peerIndex]
	var entries []LogEntry
	prevLogIndex := -1
	prevLogTerm := -1
	if thisNextIndex >= rf.Log.Length() {
		entries = make([]LogEntry, 0)
	} else {
		entries = rf.Log.entries[thisNextIndex:]
	}
	if thisNextIndex != 0 {
		prevLogIndex = thisNextIndex - 1
		prevLogTerm = rf.Log.Index(prevLogIndex).Term
	}
	ret := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.Log.CommitIndex,
		// index of previous entry of this sending package
		PrevLogIndex: prevLogIndex,
		// term of previous entry of this sending package
		PrevLogTerm: prevLogTerm,
		// sending package
		Entries: entries,
	}
	rf.printInfo("sending", len(entries), "entries to peer", peerIndex, "at nextIndex", thisNextIndex)
	rf.Log.Unlock()
	return ret
}

func (rf *Raft) SendAppendEntriesToAll() {
	call := NewAppendEntriesCall(rf)
	CallAsyncRpc(call)
}

func (rf *Raft) initiateElection() {
	rf.votedFor = rf.me
	rf.currentTerm++
	call := NewRequestVoteCall(rf, rf.Log.NewRequestVoteArgs(rf.currentTerm, rf.me))
	CallAsyncRpc(call)
	call.Wait()
	//time.Sleep(time.Duration(rf.TimeParams.requestVoteRandMax) * time.Millisecond)
}

/*
	peer state changes
	public methods must be locked
*/
func (rf *Raft) toLeader() {
	if rf.MyState.IsLeader() {
		return
	}
	rf.printInfo("set peer state to leader")
	rf.MyState = NewLeaderState(rf)
	rf.votedFor = rf.me
	rf.peerLogStates.SetAllNextIndex(rf.Log.Length())
}

func (rf *Raft) ToLeader() {
	rf.mu.Lock()
	rf.toLeader()
	rf.mu.Unlock()
}

func (rf *Raft) toFollower(peerId int) {
	rf.printInfo("set peer state to follower")
	rf.MyState = NewFollowerState(rf)
	rf.votedFor = peerId
}

func (rf *Raft) ToFollower(peerId int) {
	rf.mu.Lock()
	rf.toFollower(peerId)
	rf.mu.Unlock()
}

func (rf *Raft) toCandidate() {
	rf.MyState = NewRaftCandidate(rf)
	rf.votedFor = -1
	rf.printInfo("set peer state to candidate")
}

func (rf *Raft) ToCandidate() {
	rf.mu.Lock()
	rf.toCandidate()
	rf.mu.Unlock()
}

func (rf *Raft) LockPeerState() {
	rf.mu.Lock()
}

func (rf *Raft) UnlockPeerState() {
	rf.mu.Unlock()
}

// run state procedure
func (rf *Raft) RunStateProcedure() {
	rf.printInfo("state procedure runner thread started")
	for {
		// run indefinitely
		rf.LockPeerState()
		rf.MyState.Run()
		rf.UnlockPeerState()
	}
}

func (rf *Raft) RunAsyncProcedure() {
	rf.printInfo("async procedure runner thread started")
	for {
		rf.taskQueue.RunOne()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := NewRaft(
		applyCh,
		peers,
		persister,
		me,
		NewRaftTime(80, 200, 500),
	)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.RunStateProcedure()
	go rf.RunAsyncProcedure()
	go rf.Log.ApplyWorker()

	return rf
}
