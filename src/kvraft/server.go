package kvraft

import (
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (s *StateMachine)Apply(rc Op) RaftCommandResp {
	res := RaftCommandResp{}
	if rc.CmdType == RaftTypeGet {
		val, err := s.Get(rc.Key)
		res.Val = val
		res.Err = err
	} else if rc.CmdType == RaftTypePut {
		err := s.Put(rc.Key, rc.Val)
		res.Err = err
	} else if rc.CmdType == RaftTypeAppend {
		err := s.Append(rc.Key, rc.Val)
		res.Err = err
	}
	return res
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	notifyChs     map[int]chan RaftCommandResp
	lastAppliedId int

	stateMachine *StateMachine
	duplicateReqM map[int]LastRaftCommandResp // 某个clinet 的最后一条消息的结果和 seqId
}

// 服务端通过key 获取val
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 调用 *raft.Raft 的 start 方法将 args 的 cmd 通过raft 机制处理
	// 根据raft.start 的返回结果，如果不是leader， 则返回错误 ErrWrongLeader
	// 根据raft.start 的返回结果的index， lock， unlock 通过一个 chan 获取执行结果；
	// 带超时的 select 机制， time.After()
	index, _, isLeader := kv.rf.Start(Op{CmdType: RaftTypeGet, Key: args.Key})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyCh := kv.getNotifyChanel(index)
	kv.mu.Unlock()

	defer func() {
		// 删除 index 对应的 chan
		kv.mu.Lock()
		kv.removeNotifyChanel(index)
		kv.mu.Unlock()
		return
	}()

	select {
	case result := <- notifyCh:
		reply.Value = result.Val
		reply.Err = result.Err
		return

	case <-time.After(TimeOut):
		reply.Err = ErrTimeOut
		return
	}
	return
}

// 服务端通过 key val，写入或追加数据
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 判断请求是否重复， 如果重复，则返回err
	// 调用 *raft.Raft 的 start 方法将 args 的 cmd 通过raft 机制处理
	// 根据raft.start 的返回结果，如果不是leader， 则返回错误 ErrWrongLeader
	// 根据raft.start 的返回结果的index， lock， unlock 通过一个 chan 获取执行结果；
	// 带超时的 select 机制， time.After()

	if kv.isRaftCommandDuplicate(args.ClientId, args.SeqId) {
		reply.Err = kv.duplicateReqM[args.ClientId].rc.Err
		return
	}

	index, _, isLeader := kv.rf.Start(Op{
		CmdType:RaftTypePut,
		Key: args.Key,
		Val: args.Value,
		ClientId: args.ClientId,
		SeqId: args.SeqId,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyCh := kv.getNotifyChanel(index)
	kv.mu.Unlock()

	defer func() {
		// 删除 index 对应的 chan
		kv.mu.Lock()
		kv.removeNotifyChanel(index)
		kv.mu.Unlock()
		return
	}()

	select {
	case result := <- notifyCh:
		//reply.Value = result.val
		reply.Err = result.Err
		return

	case <-time.After(TimeOut):
		reply.Err = ErrTimeOut
		return
	}
	// 删除 index 对应的 chan
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) getNotifyChanel(index int) chan RaftCommandResp {
	if _, ok := kv.notifyChs[index]; !ok {
		kv.notifyChs[index] = make(chan RaftCommandResp)
	}
	return kv.notifyChs[index]
}

func (kv *KVServer) removeNotifyChanel(index int) {
	delete(kv.notifyChs, index)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.stateMachine = NewStateMachine()
	kv.notifyChs = make(map[int]chan RaftCommandResp)
	kv.duplicateReqM = make(map[int]LastRaftCommandResp)
	// You may need initialization code here.

	go kv.applyKvRaftTask()
	return kv
}

// 定时task， 接受raft 的应用 cmd，将cmd 应用到 stateMachine
func (kv *KVServer)applyKvRaftTask() {
	for !kv.killed() {
		// select { case message := <- kv.applyCh, 接受消息
		// 如果 commandvalid 则继续处理
		// 如果是已处理过的消息，commandindex < lastApplyed; 则忽略 continue
		// 记录 lastApplyed
		// 如果 op 是 不是get， 则校验是否请求重复， 如果重复直接返回; 否则将op 应用到状态机， 返回reply
		var result RaftCommandResp
		select {
		case message := <- kv.applyCh: // 从applyCh 获取待应用的 rafft 日志
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex < kv.lastAppliedId {
					kv.mu.Unlock()
					continue
				}
				rc := message.Command.(Op)

				if rc.CmdType != RaftTypeGet && kv.isRaftCommandDuplicate(rc.ClientId, rc.SeqId) {
					result = kv.duplicateReqM[rc.ClientId].rc
				} else {
					result = kv.stateMachine.Apply(rc)
					if rc.CmdType != RaftTypeGet {
						kv.duplicateReqM[rc.ClientId] = LastRaftCommandResp{rc.SeqId,result}
					}
				}
				// 判断如果是leader， 则通过 notifyChanle 将结果返回客户端
				if _, isLeader := kv.rf.GetState(); isLeader {
					notifyCh := kv.getNotifyChanel(message.CommandIndex)

					notifyCh <- result
				}

				kv.mu.Unlock()
			}
		}
	}
}

// 判断一个client的 一个seqId 的操作是重复的发送
func (kv *KVServer) isRaftCommandDuplicate(clientId int, seqId int) bool {
	// 如果client 对应的 LastRaftCommandResp 存在，且seqId小于或等于缓存的 LastRaftCommandResp 的seqId，
	// 则认为是重复的请求
	lastInfo, ok := kv.duplicateReqM[clientId]
	if ok && lastInfo.SeqId <= seqId {
		return true
	}
	return false
}