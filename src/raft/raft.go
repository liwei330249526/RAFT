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
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
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
const replicaInterval = time.Millisecond * 200 // 比选举下届要小，才能抑制选举

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


	logs []Entry
	next []int // 日志匹配试探点
	match []int // 日志同步成功后的匹配点
	commitId int
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (PartC).
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
	// Your code here (PartC).
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
	// Your code here (PartD).

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

	// Your code here (PartB).

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

// 转为跟随者
func (rf *Raft)becomeFollower(term int) {
	if term < rf.curTerm {
		LOG(rf.me, rf.curTerm, DError, "Can't become Follower, lower term")
		return
	}

	LOG(rf.me, rf.curTerm, DLog, "%s -> Follower, For T%d->T%d",
		rf.role, rf.curTerm, term)
	rf.role = Follower
	if term > rf.curTerm {
		rf.votedFor = -1 // 新的任期，有了投票能力
	}
	rf.curTerm = term
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
		rf.next[i] = len(rf.logs)
		rf.match[i] = 0
	}

	LOG(rf.me, rf.curTerm, DLeader, "%s -> Leader, For T%d",
		rf.role, rf.curTerm)
	rf.role = Leader
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

	rf.logs = append(rf.logs, Entry{}) //[0]位置 空的日志，避免一些边界判断.
	rf.next = make([]int, len(rf.peers))
	rf.match = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	return rf
}
