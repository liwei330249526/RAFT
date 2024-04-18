package raft

import "time"

type RequestReplicaArgs struct {
	// Your data here (PartA, PartB).
	LeaderTerm int // leader 任期
	LeaderId   int // leader Id
}

type RequestReplicaReply struct {
	// Your data here (PartA).
	Term       int // follower 任期
	Result     bool // 复制成功
}

func (rf *Raft) AppendEntries(args *RequestReplicaArgs, reply *RequestReplicaReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.curTerm
	reply.Result = false

	if args.LeaderTerm < rf.curTerm {
		LOG(rf.me, rf.curTerm, DLog, "<- S%d, reject log", args.LeaderId)
		return
	}

	rf.becomeFollower(args.LeaderTerm)
	rf.resetElection()
	reply.Result = true
	return
}

func (rf *Raft) sendRequestReplica(server int, args *RequestReplicaArgs, reply *RequestReplicaReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}



func (rf *Raft)startReplica(term int) bool {
	replicaToPeer := func(peer int, args *RequestReplicaArgs) {
		resp := &RequestReplicaReply{}
		ok := rf.sendRequestReplica(peer, args, resp)
		rf.mu.Lock()
		defer rf.mu.Unlock() // 必须加锁 --race 检测
		if !ok {
			LOG(rf.me, rf.curTerm, DLog, "->S%d, Lost", peer)
			return
		}
		//rf.mu.Lock()
		//defer rf.mu.Unlock() // 必须加锁 --race 检测

		if resp.Term > rf.curTerm {
			rf.becomeFollower(resp.Term)
			return
		}

		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果上下文发生变化则返回false
	if !(rf.role == Leader && rf.curTerm == term) {
		LOG(rf.me, rf.curTerm, DLeader, "Leader[T%d] -> [T%d]", term, rf.curTerm)
		return false
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		args := &RequestReplicaArgs{
			LeaderId: i,
			LeaderTerm: rf.curTerm,
		}
		go replicaToPeer(i, args) // 这里要开启线程执行发送, 具体的发送成功或失败，我不管
	}
	// 否则返回成功
	return true
}


func (rf *Raft) replicaTicker(term int) {
	for rf.killed() == false {

		// 是否还需要心跳复制，如果上下文不变，则我需要一只周期性执行复制；
		// 如果上下文变化，则该任期的复制动作停止
		ok := rf.startReplica(term)
		if !ok {
			return
		}

		time.Sleep(replicaInterval)
	}
}