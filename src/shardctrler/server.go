package shardctrler

import (
	"course/raft"
	"sync/atomic"
	"time"
)
import "course/labrpc"
import "sync"
import "course/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	dead    int32 // set by Kill()
	notifyChs     map[int]chan RaftCommandResp
	lastAppliedId int
	stateMachine *StateMachine
	duplicateReqM map[int]LastRaftCommandResp // 某个clinet 的最后一条消息的结果和 seqId

}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var resp RaftCommandResp
	//fmt.Println("server Join  start ", args.Servers)
	sc.OpCommon(&Op{
		CmdType: CmdTypeJoin,
		ClientId: args.ClientId,
		SeqId: args.SeqId,
		Servers: args.Servers,
	}, &resp)
	//fmt.Println("server Join ", args.Servers, resp.Err)
	reply.Err = resp.Err
	return
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	var resp RaftCommandResp
	//fmt.Println("server Leave start ", args.GIDs)
	sc.OpCommon(&Op{
		CmdType: CmdTypeLeave,
		ClientId: args.ClientId,
		SeqId: args.SeqId,
		GIDs: args.GIDs,
	}, &resp)
	//fmt.Println("server Leave ", args.GIDs, resp.Err)
	reply.Err = resp.Err
	return
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	var resp RaftCommandResp
	//fmt.Println("server Move ", args.Shard, args.GID)
	sc.OpCommon(&Op{
		CmdType: CmdTypeMove,
		ClientId: args.ClientId,
		SeqId: args.SeqId,
		Shard: args.Shard,
		GID:args.GID,
	}, &resp)
	//fmt.Println("server Move ", args.Shard, args.GID, resp.Err)
	reply.Err = resp.Err
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	var resp RaftCommandResp
	sc.OpCommon(&Op{
		CmdType: CmdTypeQuery,
		Num: args.Num,
	}, &resp)
	//fmt.Println("server Query ", args.Num, resp.Config, resp.Err)
	reply.Config = resp.Config
	reply.Err = resp.Err
	return
}

func (sc *ShardCtrler)OpCommon(args *Op, reply *RaftCommandResp) {
	sc.mu.Lock()
	if args.CmdType != CmdTypeQuery && sc.isRaftCommandDuplicate(args.ClientId, args.SeqId) {
		reply.Err = sc.duplicateReqM[args.ClientId].Rc.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	index, _, isLeader := sc.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	notifyCh := sc.getNotifyChanel(index)
	sc.mu.Unlock()

	defer func() {
		// 删除 index 对应的 chan
		sc.mu.Lock()
		sc.removeNotifyChanel(index)
		sc.mu.Unlock()
		return
	}()

	select {
	case result := <- notifyCh:
		// 如果是查询，则返回config
		reply.Config = result.Config
		reply.Err = result.Err
		return

	case <-time.After(TimeOut):
		reply.Err = ErrTimeOut
		return
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dead = 0
	sc.lastAppliedId = 0

	sc.stateMachine = NewStateMachine()
	sc.notifyChs = make(map[int]chan RaftCommandResp)
	sc.duplicateReqM = make(map[int]LastRaftCommandResp)
	//

	go sc.applyShardCtrlTask()

	return sc
}

// 定时task， 接受raft 的应用 cmd，将cmd 应用到 stateMachine
func (sc *ShardCtrler) applyShardCtrlTask() {
	for !sc.killed() {
		// select { case message := <- kv.applyCh, 接受消息
		// 如果 commandvalid 则继续处理
		// 如果是已处理过的消息，commandindex < lastApplyed; 则忽略 continue
		// 记录 lastApplyed
		// 如果 op 是 不是get， 则校验是否请求重复， 如果重复直接返回; 否则将op 应用到状态机， 返回reply
		var result RaftCommandResp
		select {
		case message := <- sc.applyCh: // 从applyCh 获取待应用的 rafft 日志
			if message.CommandValid {
				sc.mu.Lock()
				if message.CommandIndex <= sc.lastAppliedId { // todo bug:   kv.lastAppliedId <= message.CommandIndex
					sc.mu.Unlock()
					continue
				}
				sc.lastAppliedId = message.CommandIndex
				// 用户的操作
				rc := message.Command.(Op)

				if rc.CmdType != CmdTypeQuery && sc.isRaftCommandDuplicate(rc.ClientId, rc.SeqId) {
					result = sc.duplicateReqM[rc.ClientId].Rc
				} else {
					//fmt.Printf("who: %d apply: %v op is key:%s, val:%s, clientId: %d, seqId: %d\n",kv.me,rc.CmdType, rc.Key, rc.Val, rc.ClientId, rc.SeqId)
					result = sc.stateMachine.Apply(rc)
					if rc.CmdType != CmdTypeQuery {
						sc.duplicateReqM[rc.ClientId] = LastRaftCommandResp{rc.SeqId,result}
					}
				}
				// 判断如果是leader， 则通过 notifyChanle 将结果返回客户端
				if _, isLeader := sc.rf.GetState(); isLeader { // get 一直没等到，返回了也 delete 了chan， 然后这里应用了，chan一直等待
					notifyCh := sc.getNotifyChanel(message.CommandIndex)

					notifyCh <- result
				}

				sc.mu.Unlock()
			}
		}
	}
}

// 判断一个client的 一个seqId 的操作是重复的发送
func (sc *ShardCtrler) isRaftCommandDuplicate(clientId int, seqId int) bool {
	// 如果client 对应的 LastRaftCommandResp 存在，且seqId小于或等于缓存的 LastRaftCommandResp 的seqId，
	// 则认为是重复的请求
	lastInfo, ok := sc.duplicateReqM[clientId]
	//if ok && lastInfo.SeqId <= seqId { // todo bug
	if ok &&  seqId <= lastInfo.SeqId { // todo bug
		return true
	}
	return false
}

func (sc *ShardCtrler) getNotifyChanel(index int) chan RaftCommandResp {
	if _, ok := sc.notifyChs[index]; !ok {
		sc.notifyChs[index] = make(chan RaftCommandResp, 1) // todo bug:
	}
	return sc.notifyChs[index]
}

func (sc *ShardCtrler) removeNotifyChanel(index int) {
	delete(sc.notifyChs, index)
}
