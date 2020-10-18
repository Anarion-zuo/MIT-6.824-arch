package raft

type RaftRpcData struct {
	args  interface{}
	reply interface{}
}

func (rrd *RaftRpcData) GetRequestVote() (*RequestVoteArgs, *RequestVoteReply) {
	return rrd.args.(*RequestVoteArgs), rrd.reply.(*RequestVoteReply)
}

func (rrd *RaftRpcData) GetAppendEntries() (*AppendEntriesArgs, *AppendEntriesReply) {
	return rrd.args.(*AppendEntriesArgs), rrd.reply.(*AppendEntriesReply)
}

func NewRequestVoteData(args *RequestVoteArgs, reply *RequestVoteReply) *RaftRpcData {
	return &RaftRpcData{
		args:  args,
		reply: reply,
	}
}

func NewAppendEntriesData(args *AppendEntriesArgs, reply *AppendEntriesReply) *RaftRpcData {
	return &RaftRpcData{
		args:  args,
		reply: reply,
	}
}
