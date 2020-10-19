package raft

type RequestVoteCall struct {
	AsyncRpcCallAttr

	args    *RequestVoteArgs
	replies []RequestVoteReply
}

func NewRequestVoteCall(raft *Raft, args *RequestVoteArgs) *RequestVoteCall {
	return &RequestVoteCall{
		AsyncRpcCallAttr: raft.NewAsyncRpcCall(),
		args:             args,
		replies:          make([]RequestVoteReply, raft.PeerCount()),
	}
}

func (rvc *RequestVoteCall) shouldExit() bool {
	if rvc.MustExit {
		return true
	}
	return false
}

func (rvc *RequestVoteCall) makeRpcCall(peerIndex int) bool {
	rvc.raft.printInfo("sending RequestVote to peer", peerIndex)
	return rvc.peers[peerIndex].Call("Raft.RequestVote", rvc.args, &rvc.replies[peerIndex])
}

func (rvc *RequestVoteCall) callback(peerIndex int) {
	rvc.raft.printInfo("RequestVote reply received from peer", peerIndex)
	if !rvc.raft.MyState.IsCandidate() {
		rvc.SetMustExit()
		return
	}
	reply := rvc.replies[peerIndex]
	if rvc.raft.tryFollowNewerTerm(peerIndex, reply.Term) {
		rvc.SetMustExit()
		return
	}
	if reply.GrantVote {
		rvc.raft.printInfo("vote granted by peer", peerIndex)
		rvc.IncrementSuccessCount()
	}
}

func (rvc *RequestVoteCall) tryEnd() bool {
	if rvc.SuccessCount > rvc.TotalCount/2 {
		rvc.raft.printInfo("#granted", rvc.SuccessCount, "in #total", rvc.TotalCount)
		rvc.SetMustExit()
		// change raft state
		rvc.raft.toLeader()
		return true
	}
	if rvc.SuccessCount+rvc.TotalCount-rvc.CurrentCount <= rvc.TotalCount/2 {
		rvc.SetMustExit()
		rvc.raft.printInfo("#granted", rvc.SuccessCount, "too few for #total - #current", rvc.TotalCount-rvc.CurrentCount)
		return true
	}
	if rvc.CurrentCount >= rvc.TotalCount {
		rvc.raft.printInfo("#granted", rvc.SuccessCount, "too few for #total", rvc.TotalCount)
		rvc.SetMustExit()
		return true
	}
	return false
}

/*
func (rf *Raft) requestVoteCallBack(ok bool, peerIndex int, args *RequestVoteArgs, reply *RequestVoteReply, requestVoteInfo *AsyncRpcCallAttr) {
	if !ok {
		fmt.Println(rf.PrefixPrint(), "cannot reach peer", peerIndex, "when requesting a vote")
	} else {
		requestVoteInfo.IncrementAliveCount()
		if rf.tryFollowNewerTerm(peerIndex, reply.Term) {
			requestVoteInfo.SetMustExit()
		} else if reply.GrantVote {
			fmt.Println(rf.PrefixPrint(), "granted a vote by peer", peerIndex)
			requestVoteInfo.IncrementSuccessCount()
			if requestVoteInfo.SuccessCount+1 > requestVoteInfo.TotalCount/2 {
				// leader claimed!
				//fmt.Println(rf.PrefixPrint(), "got", requestVoteInfo.SuccessCount, "votes in", requestVoteInfo.AliveCount, "alive peers", requestVoteInfo.TotalCount, "total peers")
				rf.MyState = LeaderState
				rf.votedFor = rf.me
				//fmt.Println(rf.PrefixPrint(), "elected leader at term", rf.currentTerm)
				rf.setLeaderNextIndex()
				requestVoteInfo.SetMustExit()
			}
		}
	}
	requestVoteInfo.IncrementCurrentCount()
}
*/
