package node

import (
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

	//heartbeat channel
	heartbeatC chan bool

	//to leader channel
	toLeaderC chan bool
}

func (rf *Raft) NewRaft(id int,nodes map[int]*Node)  {
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
				}
            //Leader状态(向其他节点发送heartbeat)
			case Leader:
				go rf.broadcastHeartbeat()
				time.Sleep(100*time.Millisecond)
			}
		}
	}()
}

func (rf *Raft) broadcastRequestVote() {
	//设置request
	var args = &rpc.VoteArgs{
		Term:      rf.currentTerm,
		CandidateId: rf.me,
	}

	//rf.nodes保存了其他node,遍历nodes并发送投票申请
	for i := range rf.nodes {
		go func(i int) {
			var reply rpc.VoteReply
			rf.sendRequestVote(i,args,&reply)
		}(i)
	}
}


//发送申请到某个节点
// serverID - serverNode 唯一标识
// args - request 内容
// reply - follower response
func (rf *Raft) sendRequestVote(serverId int,args *rpc.VoteArgs,reply *rpc.VoteReply)  {
	//创建client
	client , err := rpc2.DialHTTP("tcp",rf.nodes[serverId].address)
	if err != nil {
		log.Fatalf("dialing: %s",err.Error())
	}
	defer client.Close()

	//调用Follower节点RequestVote
	client.Call("Raft.RequestVote",args,reply)

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
func (rf *Raft) RequestVote(args *rpc.VoteArgs,reply *rpc.VoteReply)  error{
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
func (rf *Raft) broadcastHeartbeat()  {
	for i := range rf.nodes {
		args := &rpc.HeartbeatArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		go func(i int,args *rpc.HeartbeatArgs) {
			var reply rpc.HeartbeatReply
			rf.sendHeartbeat(i,args,&reply)
		}(i,args)

	}
}

func (rf *Raft) sendHeartbeat(serverId int,args *rpc.HeartbeatArgs,reply *rpc.HeartbeatReply)  {
	// 创建 RPC client
	client, err := rpc2.DialHTTP("tcp", rf.nodes[serverId].address)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	defer client.Close()
	// 调用 follower 节点 Heartbeat 方法
	client.Call("Raft.Heartbeat", args, reply)

	// 如果follower节点term大于leader节点term
	// leader节点过时，leader转变为follower状态
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
	}
}

// Heartbeat rpc method
func (rf *Raft) Heartbeat(args *rpc.HeartbeatArgs, reply *rpc.HeartbeatReply)  error{
	// 如果leader节点term小于follower节点，不做处理并返回
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return nil
	}

	// 如果leader节点term大于follower节点
	// 说明 follower 过时，重置follower节点term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	// 将当前follower节点term返回给leader
	reply.Term = rf.currentTerm

	// 心跳成功，将消息发给 heartbeatC 通道
	rf.heartbeatC <- true

	return nil
}

func (rf *Raft) Rpc(port string)  {
	err := rpc2.Register(rf)
	if err!=nil {
		log.Fatal("rpc error: ", err)
	}
	rpc2.HandleHTTP()
	go func() {
		err := http.ListenAndServe(port,nil)
		if err != nil {
			log.Fatal("listen error: ", err)
		}
	}()
}