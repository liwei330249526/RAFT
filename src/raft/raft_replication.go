package raft

import "time"

type Entry struct {
	ValidCmd bool // 是否需要应用到状态机
	Id int        // 日志索引
	Cmd interface{} // 日志， cmd
	Term int
}

type RequestReplicaArgs struct {
	// Your data here (PartA, PartB).
	LeaderTerm int // leader 任期
	LeaderId   int // leader Id

	PreLogId int   // 上一条日志的id
	PreTerm int    // 上一条日志的term
	Logs []Entry   // 本次发送日志的内容
}

type RequestReplicaReply struct {
	// Your data here (PartA).
	Term       int // follower 任期
	Result     bool // 复制成功
}

func (rf *Raft) AppendEntries(args *RequestReplicaArgs, reply *RequestReplicaReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
    // 默认消息

    LOG(rf.me, rf.curTerm, DDebug, "<- S%d, Recive log, Pre[%d]T%d, len()=%d", args.PreLogId, args.PreTerm, len(args.Logs))

	reply.Term = rf.curTerm
	reply.Result = false

    // 任期对齐
	if args.LeaderTerm < rf.curTerm {
		LOG(rf.me, rf.curTerm, DLog, "<- S%d, reject log", args.LeaderId)
		return
	}

	rf.becomeFollower(args.LeaderTerm)

	// 如果 args 的preId 大于本日志 len， 则返回false。 ;
	if args.PreLogId >= len(rf.logs) {
		LOG(rf.me, rf.curTerm, DLog, "Reject S%d log, PreLogId:%d >= len me log %d ", args.LeaderId, args.PreLogId, len(rf.logs)-1)
		return
	}

	// 如果 args.preTerm != rf.logs[preId].term,  return false
	if args.PreTerm != rf.logs[args.PreLogId].Term {
		LOG(rf.me, rf.curTerm, DLog, "Reject S%d log, PreTerm:%d !=  me term %d ", args.LeaderId, args.PreTerm, rf.logs[args.PreLogId].Term)
		return
	}

	// append 日志， append(rf.logs[args.preid+1], args. logs)
	rf.logs = append(rf.logs[args.PreLogId+1:], append([]Entry{}, args.Logs...)...)

	// todo()：handle leader commit

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

		//4 reply 处理，如果不成功，则更新next数组，尝试再次试探，这里每次回退一个term，即 next[peer]值设置为了该term 的第一条日志。
		//如果成功则更新match数组，记录这里匹配了。 算法是， arg.preid + len(arg.logs), 对本次arg 的计算。
		//不能用本地log，因为本地log可能会在本次rpc的时候， 本地log可能还会有更新。
		if resp.Result { // 算法正确，先计算 match， 根据match 赋值 next
			rf.match[peer] = args.PreLogId + len(args.Logs)
			rf.next[peer] = rf.match[peer] + 1
		} else {
			//preId := args.PreLogId
			//preTerm := args.PreTerm
			//for id := preId; id >= 0; id-- { // todo: 核对算法是否正确
			//	if preTerm != rf.logs[id].Term {
			//		break
			//	}
			//	rf.next[peer] = id
			//}

			pId := rf.next[peer]-1
			pTerm := rf.logs[pId].Term
			for ; pId > 0; pId-- {
				if pTerm != rf.logs[pId].Term {
					break
				}
			}
			rf.next[peer] = pId+1
			LOG(rf.me, rf.curTerm, DLog, "Log id not match for %d, update for %d ", args.PreTerm, rf.next[peer])
			return
		}

		//5 todo： 更新 commitindex。

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
			//1 对于自己的这个peer， 需要设置 next 和 match数组； 因为commit的时候会用到；意思是日志复制到自己了。
			rf.next[rf.me] = len(rf.logs)
			rf.match[rf.me] = len(rf.logs)-1
			continue
		}

		preId := rf.next[i] - 1
		preTerm := rf.logs[preId].Term
		args := &RequestReplicaArgs{
			LeaderId: i,
			LeaderTerm: rf.curTerm,
			//2 对于其他peer，则构造试探匹配点； preid， preterm， 发送 entrys
			PreTerm: preTerm,
			PreLogId: preId,
			Logs: append([]Entry{}, rf.logs[preId+1:]...),
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