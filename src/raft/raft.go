package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	//"6.5840/labgob"
	//"6.5840/labrpc"
	"labrpc"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Cmd int
	Index int
	Term int
}

const (
	Candidate = 1
	Follower = 2
	Leader = 3
)

// Raft A Go object implementing a single Raft peer.
// 实现一个 raft 对等端
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	labrpc.ClientEnd
	peers     []*labrpc.ClientEnd // RPC end points of all peers   所有对等端的RPC端点
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]      本raft 实例的对等端
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 查看本文的图2，了解Raft服务器必须维护的状态。
	// 所有服务器上持久存在的
	currentTerm int 			// 服务器最后一次直到的任期号, 初始化为0, 递增
	votedFor	int				// 当前任期获得选票的候选人id, nil, 如果没有的话
	log 		[]LogEntry			// 日志条目, 每个条目包含状态机执行的命令, 和收到条目时的任期

	// 所有服务器上经常变的
	commitIndex int				// 已知的最大的已经被提交的日志条目的索引值, 初始化为0, 递增
	lastApplied int				// 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）

	// 在领导人里经常改变的 （选举后重新初始化）
	nextIndex []int				// 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
	matchIndex [] int			// 对于每一个服务器，已经复制给他的日志的最高索引值

	// tem
	raftState int
	receive  chan int
	electTimer *time.Timer
	heatBeatsTimer *time.Timer
	voted int
	leaderId int
	lastRecieveLeaderHeatBeatTime time.Time
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.raftState == Leader {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 参数
	Term int            // 候选人的任期号
	CandidateId int     // 请求选票的候选人的 Id
	LastLogIndex int    // 候选人的最后日志条目的索引值
	LastLogTerm  int    // 候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	// 返回值
	Term int          // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool   // 候选人赢得了此张选票时为真
}

type AppendEntriesArgs struct {
	Term int		  // 领导人的任期号
	LeaderId int      // 领导人的 Id，以便于跟随者重定向请求
	PrevLogIndex int  // 新的日志条目紧随之前的索引值
	PrevLogTerm int   // prevLogIndex 条目的任期号
	Entries []LogEntry  // 准备存储的日志条目（表示心跳时为空；一次性发送多个是为了提高效率）
	LeaderCommit int  // 领导人已经提交的日志的索引值
}

type AppendEntriesReply struct {
	Term int          // 当前的任期号，用于领导人去更新自己
	Success	bool      // 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
}


// RequestVote example RequestVote RPC handler.
// 处理器, 收到别人的投票请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 1. 如果  term < currentTerm  返回 false （5.2 节）
	rf.resetElectTimer()				// 收到消息, 改变时钟, 或发送消息

	if args.Term < rf.currentTerm {		// 如果对面term 较小, 则不投票
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("我是=%d, 对面是=%d, 我的任期是=%d, 对面的任期是=%d, 我投了反对票=%v", rf.me, args.CandidateId, rf.currentTerm, args.Term, reply.VoteGranted)
		return
	} else {
		rf.currentTerm = args.Term		// 更新任期

		// 2. 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给它（5.2 节，5.4 节)
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			DPrintf("我是=%d, 对面是=%d, 我的任期是=%d, 对面的任期是=%d, 我投了赞成票=%v", rf.me, args.CandidateId, rf.currentTerm, args.Term, reply.VoteGranted)
			return
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("我是=%d, 对面是=%d, 我的任期是=%d, 对面的任期是=%d, 我投了反对票=%v, 可能我已经投了其他人=%d", rf.me, args.CandidateId, rf.currentTerm, args.Term, reply.VoteGranted, rf.votedFor)
		return
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// 远程调用, 上面的 RequestVote
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries 每次收到心跳, 应该记录本次心跳的时间戳, 定期扫描该时间戳, 如果下次扫描该时间戳的时候, 发现相隔时间较大, 说明比较长的时间没有收到心跳信息了
// 则将自己转变为 Candidate
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)  {
	if args.Entries == nil {
		// 这是一个心跳, leader 是对方
		rf.lastRecieveLeaderHeatBeatTime = time.Now()
		rf.resetElectTimer()
		rf.leaderId = args.LeaderId
		rf.raftState = Follower

		reply.Term = rf.currentTerm
		reply.Success = true
	}
	DPrintf("我是=%d, 我收到了leader=%d的心跳, 我现在是=%d 了", rf.me, args.LeaderId, rf.raftState)
	return
}

func (rf *Raft) sendRequestAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 如果是leader, 发送心跳,
// 如果不是leader, 则 heatBeatsTimer 停止
func (rf *Raft) heartBeats() {
	for rf.killed() == false {
		DPrintf("我是=%d, 现在的状态是%d", rf.me, rf.raftState)
		if rf.raftState != Leader { // 如果不是 candidate 则不能发起投票
			rf.heatBeatsTimer.Stop()
		}
		DPrintf("我是=%d, 开始等待心跳", rf.me)
		select {
		case <-rf.heatBeatsTimer.C:
			DPrintf("我是=%d, 我现在是leader 了, 我要发起心跳", rf.me)
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				req := AppendEntriesArgs{
					Term:rf.currentTerm,
					LeaderId:rf.me,
					PrevLogIndex:0,
					Entries:[]LogEntry{},
					LeaderCommit:0,
				}
				reply := AppendEntriesReply{}
				rf.sendRequestAppendEntries(i, &req, &reply)
			}
		}

		rf.resetElectTimer()										// 一次心跳结束, 重置下次选举和心跳心跳计时器
		rf.heatBeatsTimer.Reset(time.Millisecond * 50)
		DPrintf("我是=%d, 下一轮心跳", rf.me)

	}
	return
}

func (rf *Raft) checkHeartBeats() {
	checkTimer := time.NewTimer(time.Millisecond * 30)			// 30 毫秒过来检查一次
	for rf.killed() == false {

		// 如果自己 follower, 则检测leader 的心跳, 如果心跳暂停了, 则自己转化为 Candidate

		select {
		case <- checkTimer.C:
			if time.Now().After(rf.lastRecieveLeaderHeatBeatTime.Add(time.Millisecond * 50)) {
				if rf.raftState == Follower {
					rf.raftState = Candidate
				}
			}
			checkTimer.Reset(time.Millisecond * 30)
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getElectRand() time.Duration {
	ms := 250 + (rand.Int63() % 150)
	return time.Millisecond * time.Duration(ms)
}

func (rf *Raft) resetElectTimer() {
	electRand := rf.getElectRand()
	rf.electTimer.Reset(electRand)
	return
}



func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		// 是否一个leader 选举应该开始了

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// 暂停50到350毫秒之间的随机时间量。

		if rf.raftState != Candidate { // 如果不是 candidate 则不能发起投票
			rf.electTimer.Stop()
		}


		llindex := 0
		llterm := 0
		if len(rf.log) != 0 {
			llindex = rf.log[len(rf.log)].Index
			llterm = rf.log[len(rf.log)].Term
		}

		rf.resetElectTimer()						    // 投票的随机超时

		select {
		case <-rf.electTimer.C:							// 计时器到期, 开始发送投票请求
			DPrintf("我是%d, 选举 timed out 了", rf.me)
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.raftState = Candidate
			rf.voted = 1

			for i := 0; i < len(rf.peers); i++ {		// 给每个对等端发送投票请求
				if i == rf.me {
					continue
				}
				if rf.raftState != Candidate {		// 如果不是 candidate 则不能发起投票
					rf.resetElectTimer()
					break
				}

				args := RequestVoteArgs{
					Term: rf.currentTerm,
					CandidateId:rf.me,
					LastLogIndex: llindex,
					LastLogTerm: llterm,
				}
				reply := RequestVoteReply{}

				sentRes :=  rf.sendRequestVote(i, &args, &reply)
				if !sentRes {
					DPrintf("我是=%d, 对面是=%d, 我发送投票请求, 发送失败了", rf.me, i)
				}
				if reply.VoteGranted {				        // 我获得一票
					rf.voted++
					DPrintf("我是=%d, 对面是=%d, 我获得了赞成票", rf.me, i)
					if rf.voted > len(rf.peers) / 2 {		// 我得到大多投票
						// 我变成领导者, 开始发送心跳, appendEntry
						DPrintf("我是=%d, 我现在是leader 了", rf.me)
						rf.raftState = Leader
						rf.leaderId = rf.me
						rf.heatBeatsTimer.Reset(time.Millisecond * 50)
						break
					}
				} else {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}
				}
			}


		}
	}

}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// 使服务或测试人员希望创建Raft服务器。所有Raft服务器（包括此服务器）的端口都在对等方[]中。
// 这台服务器的端口是 peers[me]。所有服务器的对等方[]阵列具有相同的顺序。
// persister是该服务器保存其持久状态的地方，并且最初保存最近保存的状态（如果有的话）。
// applyCh是测试人员或服务希望Raft在其上发送ApplyMsg消息的通道。
// Make（）必须快速返回，因此它应该为任何长时间运行的工作启动goroutine。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1

	// Your initialization code here (2A, 2B, 2C).
	// Make（）以创建一个后台goroutine，当有一段时间没有收到其他同行的消息时，通过发送RequestVote RPC定期启动领导人选举。
	// 通过这种方式，如果已经有了领导者，同龄人将了解谁是领导者，或者自己成为领导者。实现RequestVote（）RPC处理程序，以便服务器可以相互投票。
	rf.raftState = Candidate
	rf.electTimer = time.NewTimer(time.Millisecond * 500)
	rf.heatBeatsTimer = time.NewTimer(time.Millisecond * 50)
	// initialize from state persisted before a crash
	// 从持久里面初始化宕机前的状态
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// 开启协程, 开始投票
	go rf.ticker()
	go rf.heartBeats()
	go rf.checkHeartBeats()
	return rf
}
