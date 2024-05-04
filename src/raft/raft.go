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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
)

const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond

	//replicateInterval time.Duration = 70 * time.Millisecond
	replicateInterval time.Duration = 30 * time.Millisecond
)

const (
	InvalidTerm  int = 0
	InvalidIndex int = 0
)

type Role string

// 定义 raft 3 种角色
const (
	Follower Role = "Follower"
	Candidate Role = "Candidate"
	Leader Role = "Leader"
)

const minElectionTimeout = time.Millisecond * 250
const maxElectionTimeout = time.Millisecond * 400
const replicaInterval = time.Millisecond * 80 // 比选举下届要小，才能抑制选举

func (rf *Raft) logString() string {
	preTerm := rf.logs.lastIncludeTerm
	preStart := rf.logs.lastIncludeIndex

	ret := ""
	for i := 0; i < len(rf.logs.tailLog); i++ {
		if rf.logs.tailLog[i].Term != preTerm {
			ret += fmt.Sprintf("[%d - %d]T%d",
				preStart, i-1 + rf.logs.lastIncludeIndex, preTerm)
			preTerm = rf.logs.tailLog[i].Term
			preStart = i + rf.logs.lastIncludeIndex
		}
	}
	ret += fmt.Sprintf("[%d - %d]T%d",preStart, len(rf.logs.tailLog) + rf.logs.lastIncludeIndex, preTerm)
	return ret
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (a ApplyMsg)String() string {
	if a.CommandValid {
		return fmt.Sprintf(" apply log %d, %v",a.CommandIndex, a.Command)

	} else {
		return fmt.Sprintf(" apply snapshot %d %d", a.SnapshotIndex, a.SnapshotTerm)
	}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role Role // 出事为Follower
	curTerm int // 初始为0
	votedFor int // 出事为 -1
	electionStartTime time.Time
	electionTimeOut time.Duration


	logs        *RaftLog
	next        []int // 日志匹配试探点
	match       []int // 日志同步成功后的匹配点

	lastApplyedId int  // 上次应用的日志index
	committedId int // 已提交的日志index
	applyCh chan ApplyMsg
	applyCond *sync.Cond
	snapPend bool // 正在应用日志，优先应用日志
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//var term int
	//var isleader bool
	// Your code here (PartA).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.curTerm, rf.role == Leader
}

func (rf *Raft) GetRaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
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
	//index := -1
	//term := -1
	//isLeader := true

	// Your code here (PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, -1, false
	}

	cmd := Entry{ // 问题， id， 和term 怎么赋值
		ValidCmd:true,
		Id: rf.logs.size(),
		Cmd: command,
		Term:rf.curTerm,
	}

	rf.logs.append(cmd)
	rf.persist()
	LOG(rf.me, rf.curTerm, DLeader, "Leader accept log [%d]T%d, %v", cmd.Id, cmd.Term, cmd)

	return  cmd.Id , cmd.Term, true
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

// 转为跟随者
func (rf *Raft)becomeFollower(term int) {
	if term < rf.curTerm {
		LOG(rf.me, rf.curTerm, DError, "Can't become Follower, lower term")
		return
	}

	LOG(rf.me, rf.curTerm, DLog, "%s -> Follower, For T%d->T%d",
		rf.role, rf.curTerm, term)
	rf.role = Follower
	sholdPersist := false
	if term > rf.curTerm {
		sholdPersist = true
		rf.votedFor = -1 // 新的任期，有了投票能力
	}
	rf.curTerm = term
	if sholdPersist {
		rf.persist()
	}

	return
}
// 转为候选者
func (rf *Raft)becomeCandidate() {
	if rf.role == Leader {
		LOG(rf.me, rf.curTerm, DError, "Leader can't become Candidate")
		return
	}

	LOG(rf.me, rf.curTerm, DVote, "%s -> Candidate, For T%d->T%d",
		rf.role, rf.curTerm, rf.curTerm+1)
	rf.role = Candidate
	rf.curTerm++
	rf.votedFor = rf.me
	rf.persist()
	return
}

// 转为leader
func (rf *Raft)becomeLeader() {
	if rf.role != Candidate {
		LOG(rf.me, rf.curTerm, DLeader,
			"%s, Only candidate can become Leader", rf.role)
		return
	}

	//3 next index 在 becomeleader的时候初始化，视图在当选leader的term内有效。
	//遍历next 数组，将每个peer 的值设置为leader 日志长度，len(log)，即从这个位置试探，
	//如果不行则往前缩; 悲观下，最开始是匹配的。
	//Matchindex 设置为0，leader上台后，不清楚和谁匹配多少。
	for i := 0; i < len(rf.next); i++ {
		rf.next[i] = rf.logs.size()
		rf.match[i] = 0
	}

	LOG(rf.me, rf.curTerm, DLeader, "%s -> Leader, For T%d",
		rf.role, rf.curTerm)
	rf.role = Leader
}

func (rf *Raft) FirstIndexOfTerm(term int) int {


	//for ;id >= 0; id-- {
	//	if rf.logs[id].Term != term { // id 的日志是前一个任期的了，则返回id+1
	//		break
	//	}
	//}
	//return id+1

	for i, entry := range rf.logs.tailLog {
		if entry.Term == term {
			return i + rf.logs.lastIncludeIndex
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}


// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (PartA, PartB, PartC).
	rf.curTerm = 0
	rf.role = Follower
	rf.votedFor = -1

	rf.logs = NewLog(InvalidIndex, InvalidTerm, nil, nil)
	//rf.logs.append(Entry{})
	//rf.logs = append(rf.logs, Entry{}) //[0]位置 空的日志，避免一些边界判断.
	rf.next = make([]int, len(rf.peers))
	rf.match = make([]int, len(rf.peers))

	rf.lastApplyedId = 0 // todo: 初始化为0
	rf.committedId = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.snapPend = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	go rf.applyTicker()

	return rf
}
