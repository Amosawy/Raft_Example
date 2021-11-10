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
}

type HeartbeatReply struct {
	// 当前 follower term
	Term int
}