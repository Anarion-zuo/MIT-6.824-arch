package raft

type AppendEntriesCall struct {
	AsyncRpcCallAttr

	args    []AppendEntriesArgs
	replies []AppendEntriesReply
}

func (aec *AppendEntriesCall) makeRpcCall(peerIndex int) bool {
	aec.args[peerIndex] = *aec.raft.newAppendEntriesArgs(peerIndex)
	return aec.peers[peerIndex].Call("Raft.AppendEntries", &aec.args[peerIndex], &aec.replies[peerIndex])
}

func (aec *AppendEntriesCall) shouldExit() bool {
	if aec.MustExit {
		return true
	}
	return false
}

func (aec *AppendEntriesCall) callback(peerIndex int) {
	if !aec.raft.MyState.IsLeader() {
		aec.SetMustExit()
		return
	}
	aec.raft.printInfo("heartbeat received from peer", peerIndex)
	reply := &aec.replies[peerIndex]
	if aec.raft.tryFollowNewerTerm(peerIndex, reply.Term) {
		aec.SetMustExit()
		return
	}
	aec.raft.printInfo("peer", peerIndex, "received", len(aec.args[peerIndex].Entries), "entries")
	for reply.Success == false {
		aec.raft.peerLogStates.Less(peerIndex)
		/*
			nextIndex := aec.raft.Log.lastTermIndex(0, reply.ConflictTerm)
			if nextIndex == -1 {
				aec.raft.peerLogStates.NextIndex[peerIndex] = reply.ConflictIndex
			} else {
				aec.raft.peerLogStates.NextIndex[peerIndex] = nextIndex
			}
		*/
		aec.raft.printInfo("peer", peerIndex, "refused entries sent, must decrement nextIndex to", aec.raft.peerLogStates.NextIndex[peerIndex])
		ok := aec.makeRpcCall(peerIndex)
		if ok == false {
			return
		}
		if !aec.raft.MyState.IsLeader() {
			aec.SetMustExit()
			return
		}
		if aec.raft.tryFollowNewerTerm(peerIndex, reply.Term) {
			aec.SetMustExit()
			return
		}
	}
	aec.raft.peerLogStates.More(peerIndex, len(aec.args[peerIndex].Entries))
	aec.raft.printInfo("peer", peerIndex, "log updated to", aec.raft.peerLogStates.matchIndex[peerIndex])
}

func (aec *AppendEntriesCall) tryEnd() bool {
	if aec.CurrentCount >= aec.TotalCount {
		aec.SetMustExit()
		aec.raft.TryCommit(aec)
		return true
	}
	return false
}

func NewAppendEntriesCall(raft *Raft) *AppendEntriesCall {
	return &AppendEntriesCall{
		AsyncRpcCallAttr: raft.NewAsyncRpcCall(),
		args:             make([]AppendEntriesArgs, raft.PeerCount()),
		replies:          make([]AppendEntriesReply, raft.PeerCount()),
	}
}

/*
func (rf *Raft) appendEntriesCallBack(ok bool, peerIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply, info *AsyncRpcCallAttr) {
	if ok {
		info.SetAliveHost(peerIndex)
		if rf.tryFollowNewerTerm(peerIndex, reply.Term) {
			info.SetMustExit()
		} else {
			// decrement and retry
			for reply.Success == false {
				//rf.logMutex.Lock()
				if rf.MyState != LeaderState {
					info.SetMustExit()
					return
				}
				rf.NextIndex[peerIndex]--
				args = rf.newAppendEntriesArgs(peerIndex)
				fmt.Println(rf.PrefixPrint(), "got false heartbeat reply from peer", peerIndex, ", must decrement NextIndex then try again")
				//rf.logMutex.Unlock()
				// retry
				reply = &AppendEntriesReply{}
				ok = rf.sendAppendEntries(peerIndex, args, reply)
				if !ok {
					break
				}
				if rf.tryFollowNewerTerm(peerIndex, reply.Term) {
					info.SetMustExit()
					return
				}
			}
			if ok {
				//rf.logMutex.Lock()
				// update NextIndex, matchIndex

				//rf.logMutex.Unlock()
			}
		}
	} else {
		//fmt.Println(rf.PrefixPrint(), "found peer", peerIndex, "unreachable when sending heartbeats")
	}
	info.IncrementCurrentCount()
	if ok {
		fmt.Println(rf.PrefixPrint(), "got reply on AppendEntries #current", info.CurrentCount, "#total", info.TotalCount)
	} else {
		fmt.Println(rf.PrefixPrint(), "got timeout on AppendEntries #current", info.CurrentCount, "#total", info.TotalCount)
	}
	if info.MustExit == false {
		rf.leaderTryCommit(info)
	}
}
*/
