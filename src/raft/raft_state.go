package raft

type RaftState interface {
	IsLeader() bool
	IsCandidate() bool
	ToString() string
	Run()
}

type RaftStateAttr struct {
	raft *Raft
}

func NewStateCommon(raft *Raft) RaftStateAttr {
	return RaftStateAttr{
		raft: raft,
	}
}

type RaftLeader struct {
	RaftStateAttr
}

type RaftFollower struct {
	RaftStateAttr
}

type RaftCandidate struct {
	RaftStateAttr
}

/*
	Constructors
*/
func NewLeaderState(raft *Raft) *RaftLeader {
	return &RaftLeader{
		RaftStateAttr: NewStateCommon(raft),
	}
}

func NewFollowerState(raft *Raft) *RaftFollower {
	return &RaftFollower{
		RaftStateAttr: NewStateCommon(raft),
	}
}

func NewRaftCandidate(raft *Raft) *RaftCandidate {
	return &RaftCandidate{
		RaftStateAttr: NewStateCommon(raft),
	}
}

/*
	check leader
*/
func (*RaftFollower) IsLeader() bool {
	return false
}

func (*RaftCandidate) IsLeader() bool {
	return false
}

func (*RaftLeader) IsLeader() bool {
	return true
}

/*
	check candidate
*/
func (*RaftFollower) IsCandidate() bool {
	return false
}

func (*RaftLeader) IsCandidate() bool {
	return false
}

func (*RaftCandidate) IsCandidate() bool {
	return true
}

/*
	Print
*/
func (leader *RaftLeader) ToString() string {
	return "Leader"
}

func (follower *RaftFollower) ToString() string {
	return "Follower"
}

func (candidate *RaftCandidate) ToString() string {
	return "Candidate"
}

/*
	Run roll
*/
// actions are ignored for inconsistent raft state
func (leader *RaftLeader) Run() {
	leader.raft.printInfo("sending a heartbeat message to all")
	leader.raft.SendAppendEntriesToAll()
	leader.raft.printInfo("all heartbeat messages sent")
	leader.raft.UnlockPeerState()
	leader.raft.TimeParams.WaitHeartBeat()
	leader.raft.LockPeerState()
	// locking won't matter
	// the function exits
	// if the peer state is changed, the next roll would discover it
}

/*
func (rf *Raft) runLeader() {
	// init NextIndex
	for index, _ := range rf.nextIndex {
		rf.nextIndex[index] = len(rf.logs)
	}
	for {
		// prepare an info instance
		info := rf.NewAsyncRpcCall(len(rf.peers))
		if rf.MyState != LeaderState {
			return
		}
		// send heartbeat signal
		for peerIndex, _ := range rf.peers {
			if peerIndex == rf.me {
				continue
			}
			//fmt.Println(rf.PrefixPrint(), "sending heartbeat signal to peer", peerIndex)
			// send ahead logs
			args := rf.newAppendEntriesArgs(peerIndex)
			rf.sendAppendEntriesAsync(peerIndex, args, &AppendEntriesReply{}, info)
		}
		time.Sleep(time.Millisecond * time.Duration(rf.heartBeatWaitDuration))
	}
}
*/

func (follower *RaftFollower) Run() {
	follower.raft.printInfo("begin waiting for", follower.raft.TimeParams.heartBeatSendWait, "ms")
	follower.raft.UnlockPeerState()
	if follower.raft.TimeParams.heartBeatTimer.Wait() {
		follower.raft.LockPeerState()
		// timer expired
		follower.raft.printInfo("timer expired, becoming candidate")
		follower.raft.toCandidate()
		// this is the only way of being candidate
		// no need to worry about holding lock for this long time
		// other actions won't try to make this peer a leader
	} else {
		follower.raft.printInfo("timer cleared, following still peer", follower.raft.votedFor)
		follower.raft.LockPeerState()
	}
}

func (candidate *RaftCandidate) Run() {
	// release lock to allow peer state changes
	candidate.raft.printInfo("wait a random time then initiate an election")
	candidate.raft.UnlockPeerState()
	candidate.raft.TimeParams.WaitRandomRequestVote()
	candidate.raft.LockPeerState()
	// must check peer state for change in waiting
	if !candidate.IsCandidate() {
		return
	}
	candidate.raft.printInfo("initiate an election")
	candidate.raft.initiateElection()
}
