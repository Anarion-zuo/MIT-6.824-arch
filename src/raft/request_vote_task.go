package raft

type RequestVoteTask struct {
	RaftTaskAttr
	args  *RequestVoteArgs
	reply *RequestVoteReply
}

func (rvt *RequestVoteTask) execute() {
	rvt.executeRequestVoteRpc(rvt.args, rvt.reply)
}

func (rvt *RequestVoteTask) printThisMoreUpToDate() {
	rvt.raft.printInfo("this peer's log is newer")
}

func (rvt *RequestVoteTask) printGrantVote(peerId int) {
	rvt.raft.printInfo("grant vote to peer", peerId)
}

func (rvt *RequestVoteTask) grantVote(peerId int, reply *RequestVoteReply) {
	reply.GrantVote = true
	rvt.raft.toFollower(peerId)
	rvt.raft.TimeParams.heartBeatTimer.SetClear()
}

func (rvt *RequestVoteTask) executeRequestVoteRpc(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = rvt.raft.currentTerm
	reply.GrantVote = false

	if args.Term < rvt.raft.currentTerm {
		return
	}
	if rvt.raft.tryFollowNewerTerm(args.CandidateId, args.Term) {
		rvt.raft.printInfo("sees newer term RequestVote from peer", args.CandidateId)
		rvt.grantVote(args.CandidateId, reply)
		return
	}
	// decide vote
	if rvt.raft.votedFor < 0 || rvt.raft.votedFor == args.CandidateId {
		// check up-to-date
		if rvt.raft.Log.LastEntry().Term < args.LastLogTerm {
			// that peer has more up-to-date Log
			rvt.grantVote(args.CandidateId, reply)
			rvt.printGrantVote(args.CandidateId)
			return
		}
		if rvt.raft.Log.LastEntry().Term > args.LastLogTerm {
			// this peer has more up-to-date Log
			rvt.printThisMoreUpToDate()
			return
		}
		// Term attribute equals, comparing length
		if args.LastLogIndex <= rvt.raft.Log.Length()-1 {
			// this peer is more up-to-date
			rvt.printThisMoreUpToDate()
			return
		}
		rvt.printGrantVote(args.CandidateId)
		rvt.grantVote(args.CandidateId, reply)
		return
	}
}

func NewRequestVoteTask(raft *Raft, args *RequestVoteArgs, reply *RequestVoteReply) *RequestVoteTask {
	return &RequestVoteTask{
		RaftTaskAttr: NewRaftTaskAttr(raft),
		args:         args,
		reply:        reply,
	}
}

/*
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	RunTask(NewRequestVoteTask(rf, args, reply), &rf.taskQueue)
	return

	// default reply state
	reply.Term = rf.currentTerm
	reply.GrantVote = false
	rf.timerCleared = true

	if rf.tryDiscardOldTerm(args.CandidateId, args.Term) {
		reply.GrantVote = false
		return
	}
	if rf.tryFollowNewerTerm(args.CandidateId, args.Term) {
		reply.GrantVote = true
		return
	}

	// < 0 for not elected leader
	// == for already accepted leader
	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		// check up-to-date
		if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm {
			// this peer has more up-to-date Log
			return
		}
		if rf.logs[len(rf.logs)-1].Term < args.LastLogTerm {
			// that peer has more up-to-date Log
			reply.GrantVote = true
			return
		}
		// Term attribute equals, comparing length
		if args.LastLogIndex < len(rf.logs)-1 {
			// this peer is more up-to-date
			return
		}
		reply.GrantVote = true
		return
	}
	//fmt.Println(rf.PrefixPrint(), "with leader", rf.votedFor, "at term %v not granting vote to peer", rf.currentTerm, "at term", args.Term)
}
*/
