package raft

import (
	"fmt"
	"math/rand"
	"time"
)

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("Candidate-%d T%d, Last:[%d]T%d", args.CandidateId, args.CandidateTerm, args.LastLogId, args.LastLogTerm)
}
func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("T%d, VoteGranted: %v", reply.Term, reply.VotedGrand)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	CandidateTerm int
	CandidateId   int
	LastLogId     int // 最新的日志的id
	LastLogTerm   int // 最新的日志的term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term       int
	VotedGrand bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock() // 必须加锁 --race 检测

	LOG(rf.me, rf.curTerm, DDebug, "<- S%d, VoteAsked, Args=%v", args.CandidateId, args.String())

	reply.Term = rf.curTerm
	reply.VotedGrand = false
	if args.CandidateTerm < rf.curTerm {
		reply.VotedGrand = false
		LOG(rf.me, rf.curTerm, DVote, "%d reject vote, term %d is letter", args.CandidateId, args.CandidateTerm)
		return
	}

	if args.CandidateTerm > rf.curTerm {  // todo 检查边界，是否 ==
		rf.becomeFollower(args.CandidateTerm)
	}

	// 处理日志时，增加最新任期索引的判断, 如果投票者日志不是更新的，则返回false
	if !(rf.isCandidateMoreUp(args.LastLogId, args.LastLogTerm)) {
		indexLog := rf.logs.size()-1
		termLog := rf.logs.at(indexLog).Term
		LOG(rf.me, rf.curTerm, DVote, "%d reject vote, candidate log T[d], id[d] not up me T[%d], id[%d]",
			args.LastLogId, args.LastLogTerm, termLog, indexLog)
		return
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateTerm {
		reply.VotedGrand = false
		LOG(rf.me, rf.curTerm, DVote, "%d reject vote, already vote for other %d", args.CandidateId, rf.votedFor)
		return
	}

	LOG(rf.me, rf.curTerm, DVote, "%d voted.", args.CandidateId)
	reply.VotedGrand = true
	rf.votedFor = args.CandidateId
	rf.persist()
	rf.resetElection()
	return
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


func (rf *Raft) electionTicker() {
	for rf.killed() == false {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeout(){
			rf.becomeCandidate()
			go rf.starElection(rf.curTerm)
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 是否选举计时器超时，如果超时，则需要发起选取了
func (rf *Raft)isElectionTimeout() bool {
	return time.Since(rf.electionStartTime) > rf.electionTimeOut
}

// 重置选举超时计时器， 随机值;
func(rf *Raft)resetElection() {
	rf.electionStartTime = time.Now()
	rg := int64(maxElectionTimeout - minElectionTimeout)
	rf.electionTimeOut = time.Duration(int64(minElectionTimeout) + (rand.Int63() % rg))
}

// 保证term 任期内，选出一个leader
func (rf *Raft) starElection(term int) bool {
	// 给peer 发一个 req 请求
	voted := 0
	askVote := func(peer int, req *RequestVoteArgs) {
		resp := &RequestVoteReply{}
		ret := rf.sendRequestVote(peer, req, resp)
		rf.mu.Lock()
		defer rf.mu.Unlock() // 必须加锁 --race 检测， 必须加在上面， 因为log 中用了临街资源
		if !ret {
			LOG(rf.me, rf.curTerm,DError, "ask vote from %d err", peer)
			return
		}
		LOG(rf.me, rf.curTerm, DDebug, "-> S%d, AskVote Reply=%v", peer, resp.String())

		//rf.mu.Lock()
		//defer rf.mu.Unlock() // 必须加锁 --race 检测
		if resp.Term > term {
			LOG(rf.me, rf.curTerm,DError, "peer %d term %d bigger", peer, resp.Term)
			return
		}

		if rf.role != Candidate || rf.curTerm != term {
			LOG(rf.me, rf.curTerm,DDebug, "me role %v or term %d change, not Candidate", rf.role, rf.curTerm)
			return
		}

		if resp.VotedGrand {
			voted++
		}
		if voted > len(rf.peers) / 2 {
			rf.becomeLeader()
			go rf.replicaTicker(term)
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Candidate || rf.curTerm != term {
		LOG(rf.me, rf.curTerm,DError, "me role %v or term %d change", rf.role, rf.curTerm)
		return false
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			voted++
			continue
		}

		lastId := rf.logs.size()-1
		lastTerm := rf.logs.at(lastId).Term
		req := &RequestVoteArgs{
			CandidateTerm: rf.curTerm,
			CandidateId:   rf.me,
			LastLogId:     lastId,
			LastLogTerm:   lastTerm,
		}
		LOG(rf.me, rf.curTerm, DDebug, "-> S%d, AskVote, Args=%v", i, req.String())

		go askVote(i, req)
	}
	return true
}

// 投票者的日志是否更新
func (rf *Raft) isCandidateMoreUp(candidateIndex, candidateTerm int) bool {
	//1 任期更大的更新。
	//2 任期相等， 则日志id更大的更新。
	index := rf.logs.size()-1
	term := rf.logs.at(index).Term

	if term != candidateTerm  {
		return candidateTerm > term
	}

	return candidateIndex >= index
}