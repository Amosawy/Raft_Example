package rpc

//request
type VoteArgs struct {
	//当前任期号
	Term int
	//候选人id
	CandidateId int
}

//response
type VoteReply struct {
	//当前任期号
	// 如果其他节点大于当前Candidate节点term
	// 以便Candidate去更新自己的任期号
	Term int
	//候选人赢得此张选票为true
	VoteGranted bool
}

//request
type HeartbeatArgs struct {
	//当前leader term
	Term int
	//当前leader Id
	LeaderId int
	// 新日志之前的索引
	PrevLogIndex int
	// PrevLogIndex 的任期号
	PrevLogTerm int
	// 准备存储的日志条目（表示心跳时为空）
	Entries []LogEntry
	// Leader 已经commit的索引值
	LeaderCommit int
}

type HeartbeatReply struct {
	Term    int
	Success bool
	// 如果 Follower Index小于 Leader Index， 会告诉 Leader 下次开始发送的索引位置
	NextIndex int
}

type LogEntry struct {
	//当前Term
	LogTerm int
	//当前index
	LogIndex int
	//用户command
	LogCMD interface{}
}
