package raft

import "fmt"

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludeIndex int
	LastIncludeTerm int
	Snapshot []byte
}
func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Last: [%d]T%d", args.LeaderId, args.Term, args.LastIncludeIndex, args.LastIncludeTerm)
}

type InstallSnapshotReply struct {
	Term int
}
func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T%d", reply.Term)
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).
	//加锁
	//if index <= lastIncludeIndex   || index > commitedId, err
	//	lastIncludeIndex， lastIncludeTerm 赋值
	//snapshot 赋值
	//newLog 赋值[0] ， lastIncludeTerm
	//newLog 赋值[1:]， idx+1: 截断日志
	//persist()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.logs.lastIncludeIndex || index > rf.committedId {
		LOG(rf.me, rf.curTerm, DSnap, "Can't snapshot, index out (%d - %d]", rf.logs.lastIncludeIndex, rf.committedId)
		return
	}
	rf.logs.doSnapshot(index, snapshot)
	rf.persist()
	return
}

// 接受快照
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 默认消息
	LOG(rf.me, rf.curTerm, DSnap, "<- S%d, Append snapshot, Args=%v", args.LeaderId, args.String())
	//
	//LOG(rf.me, rf.curTerm, DDebug, "<- S%d, Recive log, Pre[%d]T%d, len()=%d, argCommitId %d , commitid %d",
	//	args.LeaderId, args.PreLogId, args.PreTerm, len(args.Logs), args.LeaderCommittedId, rf.committedId)

	reply.Term = rf.curTerm


	// 任期对齐
	if args.Term < rf.curTerm {
		LOG(rf.me, rf.curTerm, DSnap, "<- S%d, Reject Snap, Higher Term: T%d>T%d", args.LeaderId, rf.curTerm, args.Term)
		return
	}

	rf.becomeFollower(args.Term)

	//
	//if LastIncludeIndex <= rf.logs.LastIncludeIndex , 则拒绝
	//	logs 安装快照
	//持久化 persist
	//设置 snapPend，告知来了snap了，先做snap应用
	//cond.sinal()
	if args.LastIncludeIndex <= rf.logs.lastIncludeIndex {
		LOG(rf.me, rf.curTerm, DSnap, "Reject S%d snapshot, already installed [req %d <= me %d]",
			args.LeaderId, args.LastIncludeIndex, rf.logs.lastIncludeIndex)
		return
	}
	rf.logs.installSnapshot(args.LastIncludeIndex, args.LastIncludeTerm, args.Snapshot)
	rf.persist()
	rf.snapPend = true
	rf.applyCond.Signal()
	return
}


func (rf *Raft)replicaSnapshotToPeer(peer int, term int, args *InstallSnapshotArgs) {
	resp := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, resp)

	rf.mu.Lock()
	defer rf.mu.Unlock() // 必须加锁 --race 检测
	if !ok {
		LOG(rf.me, rf.curTerm, DSnap, "->S%d, Lost", peer)
		return
	}
	LOG(rf.me, rf.curTerm, DSnap, "-> S%d, Appended snapshot, Reply=%v", peer, resp.String())

	//rf.mu.Lock()
	//defer rf.mu.Unlock() // 必须加锁 --race 检测
	if resp.Term > rf.curTerm {
		rf.becomeFollower(resp.Term)
		return
	}

	// 判断任期和角色
	// 如果上下文发生变化则返回false
	if !(rf.role == Leader && rf.curTerm == term) {
		LOG(rf.me, rf.curTerm, DSnap, "Leader[T%d] -> %s[T%d]", term, rf.role, rf.curTerm)
		return
	}

	// 对peer 的快照cmd 成功，更新 match next 数组
	if rf.match[peer] < args.LastIncludeIndex {
		rf.match[peer] = args.LastIncludeIndex
		rf.next[peer] = rf.match[peer]+1
	}

	// 不需要更新commit index，因为，因为快照已经包含的是已提交的的数据。
}

// 发送快照
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
