package raft

type AppendEntriesTask struct {
	RaftTaskAttr
	args  *AppendEntriesArgs
	reply *AppendEntriesReply
}

func (aet *AppendEntriesTask) execute() {
	aet.executeAppendEntriesRpc(aet.args, aet.reply)
}

func (aet *AppendEntriesTask) executeAppendEntriesRpc(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = aet.raft.currentTerm
	reply.Success = true
	aet.raft.printInfo("heartbeat received from peer", args.LeaderId)

	// ignore old terms
	if aet.raft.currentTerm > args.Term {
		reply.Success = false
		aet.raft.printInfo("sees an old term AppendEntries from peer", args.LeaderId)
		return
	}
	if aet.raft.tryFollowNewerTerm(args.LeaderId, args.Term) {

	}
	aet.raft.TimeParams.heartBeatTimer.SetClear()
	if args.PrevLogIndex >= aet.raft.Log.Length() {
		reply.Success = false
		aet.raft.printInfo("new entries to log index", args.PrevLogIndex, "too large")
		reply.ConflictIndex = aet.raft.Log.Length()
		reply.ConflictTerm = -1
		return
	}
	if args.PrevLogIndex != -1 {
		// check when there should be a previous log entry
		if aet.raft.Log.Index(args.PrevLogIndex).Term != args.PrevLogTerm {
			reply.Success = false
			aet.raft.printInfo("new entries term", args.PrevLogTerm, "not consistent with this peer's previous log entry term", aet.raft.Log.Index(args.PrevLogIndex).Term)
			reply.ConflictTerm = aet.raft.Log.Index(args.PrevLogIndex).Term
			reply.ConflictIndex = aet.raft.Log.firstTermIndex(args.PrevLogIndex, reply.ConflictTerm)
			return
		}
	}
	// here the log can be updated
	aet.raft.printInfo("trying to append #entries", len(args.Entries))
	aet.raft.Log.UpdateLog(args.Entries, args.PrevLogIndex, args.LeaderCommit)
	// extra modifications done under candidate
	if aet.raft.MyState.IsCandidate() {
		aet.raft.currentTerm = args.Term
		aet.raft.ToFollower(args.LeaderId)
	}
}

func NewAppendEntriesTask(raft *Raft, args *AppendEntriesArgs, reply *AppendEntriesReply) *AppendEntriesTask {
	return &AppendEntriesTask{
		RaftTaskAttr: NewRaftTaskAttr(raft),
		args:         args,
		reply:        reply,
	}
}

// A non-leader should receive this
/*
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	RunTask(NewAppendEntriesTask(rf, args, reply), &rf.taskQueue)
	return
	//fmt.Println(rf.PrefixPrint(), "got heartbeat message from leader peer", args.LeaderId, "at term", args.Term)
	// default reply state
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.timerCleared = true

	if rf.tryDiscardOldTerm(args.LeaderId, args.Term) {
		reply.Success = false
		return
	}
	if rf.tryFollowNewerTerm(args.LeaderId, args.Term) {
		reply.Success = true
	}
	// check Log info
	if args.PrevLogIndex >= len(rf.logs) {
		reply.Success = false
		fmt.Println(rf.PrefixPrint(), "got new Log index", args.PrevLogIndex+1, "too large for this peer's Log length", len(rf.logs))
		return
	} else {
		if args.PrevLogIndex != -1 {
			if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
				reply.Success = false
				fmt.Println(rf.PrefixPrint(), "Log entry on PrevLogIndex term inconsistent")
				return
			}
		}
	}
	for argsEntryIndex := 0; argsEntryIndex < len(args.Entries); {
		newEntryIndex := argsEntryIndex + args.PrevLogIndex + 1
		if newEntryIndex < len(rf.logs) {
			oldEntry := &rf.logs[newEntryIndex]
			newEntry := &args.Entries[argsEntryIndex]
			// existing Log
			// check for conflict
			if oldEntry.Equals(newEntry) {
				// consistent!
				argsEntryIndex++
			} else {
				// inconsistent!
				// delete everything after current index
				rf.logs = rf.logs[:newEntryIndex]
				fmt.Println(rf.PrefixPrint(), "inconsistent with leader at Log index", newEntryIndex, "removing from then on")
			}
		} else {
			// new Log
			// append everything
			rf.logs = append(rf.logs, args.Entries[argsEntryIndex:]...)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		if args.LeaderCommit < len(rf.logs)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.logs) - 1
		}
		for ; oldCommitIndex <= rf.commitIndex; oldCommitIndex++ {
			if oldCommitIndex == 0 {
				continue
			}
			rf.applyCh <- ApplyMsg{
				CommandIndex: oldCommitIndex,
				CommandValid: true,
				Command:      rf.logs[oldCommitIndex].Command,
			}
		}

	}
	fmt.Println(rf.PrefixPrint(), "got", len(args.Entries), "new Log entries from leader peer", args.LeaderId, "committed index", rf.commitIndex)

	switch rf.MyState {
	case LeaderState:
		break
	case FollowerState:
		break
	case CandidateState:
		rf.MyState = FollowerState
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		reply.Success = true
		fmt.Println(rf.PrefixPrint(), "set leader to peer", args.LeaderId, "by heartbeat message")
		break
	default:
		panic("Invalid peer state in rpc AppendEntries!")
	}

	//rf.mu.Unlock()
}
*/
