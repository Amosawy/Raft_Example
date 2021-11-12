package node

import (
	"fmt"
	"github.com/Amosawy/Raft_Example/rpc"
	"log"
	"math/rand"
	"net/http"
	rpc2 "net/rpc"
	"time"
)

//节点结构体定义
type Node struct {
	connect bool
	address string
}

type State int

//Raft节点三种状态：Follower、Candidate、Leader
const (
	Follower State = iota + 1
	Candidate
	Leader
)

//Raft Node
type Raft struct {
	//当前节点id
	me int

	//除当前节点外其他节点信息
	nodes map[int]*Node

	//当前节点状态
	state State

	//当前节点任期
	currentTerm int

	//当前任期投票给了谁
	//未投票为-1
	votedFor int

	//当前任期获得的票数
	votedCount int

	//日志条目集合
	log []rpc.LogEntry

	//被提交的最大索引
	commitIndex int

	// 被应用到状态机的最大索引
	lastApplied int

	// 保存需要发送给每个节点的下一个条目索引
	nextIndex []int
	// 保存已经复制给每个节点日志的最高索引
	matchIndex []int

	//heartbeat channel
	heartbeatC chan bool

	//to leader channel
	toLeaderC chan bool
}

func (rf *Raft) NewRaft(id int, nodes map[int]*Node) {
	rf.me = id
	rf.nodes = nodes
}

//新建节点
func NewNode(address string) *Node {
	node := &Node{address: address}
	return node
}

//创建Raft

func (rf *Raft) Start() {
	//初始化Raft节点
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatC = make(chan bool)
	rf.toLeaderC = make(chan bool)

	//节点状态变更以及rpc处理
	go func() {
		rand.Seed(time.Now().UnixNano())
		//不间断处理节点行为和RPC
		for {
			switch rf.state {
			//Follower状态(1.heartbeat 检测 2.heartbeat 超时处理)
			case Follower:
				select {
				case <-rf.heartbeatC:
					//从heartbeatC接收到heartbeat
					log.Printf("follower-%d received heartbeat\n", rf.me)
				case <-time.After(time.Duration(rand.Intn(500-300)+300) * time.Millisecond):
					//heartbeat超时,Follower变成Candidate
					log.Printf("follower-%d heartbeat timeout", rf.me)
					rf.state = Candidate
				}
			//Candidate状态(1.为自己投票 2.向其他节点发起投票申请 3.选举超时 4.选举成功)
			case Candidate:
				log.Printf("Node: %d,i am Candidate", rf.me)
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.votedCount = 1

				//向其他节点广播投票
				go rf.broadcastRequestVote()

				select {
				//选举超时
				case <-time.After(time.Duration(rand.Intn(5000-300)+300) * time.Millisecond):
					log.Printf("Candidate-%d election timeout", rf.me)
					rf.state = Follower
				//选举成功
				case <-rf.toLeaderC:
					log.Printf("Candidate-%d became Leader", rf.me)
					rf.state = Leader

					// 初始化 peers 的 nextIndex 和 matchIndex
					rf.nextIndex = make([]int, len(rf.nodes))
					rf.matchIndex = make([]int, len(rf.nodes))

					// 为每个节点初始化nextIndex 和 matchIndex
					// 这里不考虑leader重新选举的情况
					for i := range rf.nodes {
						rf.nextIndex[i] = 1
						rf.matchIndex[i] = 0
					}

					//模拟客户端每3s发送一条command
					go func() {
						i := 0
						for {
							i++
							rf.log = append(rf.log, rpc.LogEntry{
								LogTerm:  rf.currentTerm,
								LogIndex: i,
								LogCMD:   fmt.Sprintf("user send %d", i),
							})
							time.Sleep(3 * time.Second)
						}
					}()
				}
				//Leader状态(向其他节点发送heartbeat)
			case Leader:
				go rf.broadcastHeartbeat()
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
}

func (rf *Raft) broadcastRequestVote() {
	//设置request
	var args = &rpc.VoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	//rf.nodes保存了其他node,遍历nodes并发送投票申请
	for i := range rf.nodes {
		go func(i int) {
			var reply rpc.VoteReply
			rf.sendRequestVote(i, args, &reply)
		}(i)
	}
}

//发送申请到某个节点
// serverID - serverNode 唯一标识
// args - request 内容
// reply - follower response
func (rf *Raft) sendRequestVote(serverId int, args *rpc.VoteArgs, reply *rpc.VoteReply) {
	//创建client
	client, err := rpc2.DialHTTP("tcp", rf.nodes[serverId].address)
	if err != nil {
		log.Fatalf("dialing: %s", err.Error())
	}
	defer client.Close()

	//调用Follower节点RequestVote
	client.Call("Raft.RequestVote", args, reply)

	// 如果candidate节点term小于follower节点,当前candidate节点无效,candidate节点转变为follower节点
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		return
	}

	//成功获得选票
	if reply.VoteGranted {
		// 票数加一
		rf.votedCount++
		// 获得票数大于集群的一半
		if rf.votedCount > len(rf.nodes)/2+1 {
			//发送通知给toLeaderC
			rf.toLeaderC <- true
		}
	}
}

//Follower处理投票申请
func (rf *Raft) RequestVote(args *rpc.VoteArgs, reply *rpc.VoteReply) error {
	//如果candidate节点小于follower节点任期，说明candidate节点过期，拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return nil
	}

	//follower节点未投票，投票成功
	if rf.votedFor == -1 {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return nil
	}

	//其他情况
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return nil
}

//广播heartbeat
func (rf *Raft) broadcastHeartbeat() {
	for i := range rf.nodes {

		args := &rpc.HeartbeatArgs{
			Term:         rf.currentTerm,
			LeaderCommit: rf.commitIndex,
			LeaderId:     rf.me,
		}

		// 计算 preLogIndex 、preLogTerm
		preLogIndex := rf.nextIndex[i] - 1
		// 提取 preLogIndex - baseIndex 之后的entry，发送给 follower
		if rf.getLastIndex() > preLogIndex {
			args.PrevLogIndex = preLogIndex
			args.PrevLogTerm = rf.log[preLogIndex].LogTerm
			args.Entries = rf.log[preLogIndex:]
			log.Printf("preLogIndex :%d send entries: %v\n", preLogIndex, args.Entries)
		}
		go func(i int, args *rpc.HeartbeatArgs) {
			var reply rpc.HeartbeatReply
			rf.sendHeartbeat(i, args, &reply)
		}(i, args)

	}
}

func (rf *Raft) sendHeartbeat(serverId int, args *rpc.HeartbeatArgs, reply *rpc.HeartbeatReply) {
	// 创建 RPC client
	client, err := rpc2.DialHTTP("tcp", rf.nodes[serverId].address)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	defer client.Close()
	// 调用 follower 节点 Heartbeat 方法
	client.Call("Raft.Heartbeat", args, reply)

	if reply.Success {
		if reply.NextIndex > 0 {
			rf.nextIndex[serverId] = reply.NextIndex
			rf.matchIndex[serverId] = rf.nextIndex[serverId] - 1
			// TODO
			// 如果大于半数节点同步成功
			// 1. 更新 leader 节点的 commitIndex
			// 2. 返回给客户端
			// 3. 应用状态就机
			// 4. 通知 Followers Entry 已提交
		}
	} else {
		// 如果 leader 的 term 小于 follower 的 term， 需要将 leader 转变为 follower 重新选举
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			return
		}
	}
}

// Heartbeat rpc method
func (rf *Raft) Heartbeat(args *rpc.HeartbeatArgs, reply *rpc.HeartbeatReply) error {
	// 如果leader节点term小于follower节点，不做处理并返回
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return nil
	}

	// 心跳成功，将消息发给 heartbeatC 通道
	rf.heartbeatC <- true

	//如果只是心跳
	if len(args.Entries) == 0 {
		reply.Success = true
		reply.Term = rf.currentTerm
		return nil
	}

	// 如果有 entries
	// leader 维护的 LogIndex 大于当前 Follower 的 LogIndex
	// 代表当前 Follower 失联过，所以 Follower 要告知 Leader 它当前
	// 的最大索引，以便下次心跳 Leader 返回
	if args.PrevLogIndex > rf.getLastIndex() {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return nil
	}
	rf.log = append(rf.log, args.Entries...)
	rf.commitIndex = rf.getLastIndex()
	reply.Success = true
	reply.NextIndex = rf.getLastIndex() + 1
	return nil
}

func (rf *Raft) Rpc(port string) {
	err := rpc2.Register(rf)
	if err != nil {
		log.Fatal("rpc error: ", err)
	}
	rpc2.HandleHTTP()
	go func() {
		err := http.ListenAndServe(port, nil)
		if err != nil {
			log.Fatal("listen error: ", err)
		}
	}()
}

func (rf *Raft) getLastIndex() int {
	rLen := len(rf.log)
	if rLen == 0 {
		return 0
	}
	return rf.log[rLen-1].LogIndex
}
