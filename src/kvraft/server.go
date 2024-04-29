package kvraft

import (
	"bytes"
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"fmt"
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
	if rc.CmdType == CmdTypeGet {
		val, err := s.Get(rc.Key)
		//fmt.Printf("get op is key:%s, val:%s, clientId: %d, seqId: %d\n", rc.Key, val, rc.ClientId, rc.SeqId)
		res.Val = val
		res.Err = err
	} else if rc.CmdType == CmdTypePut {
		err := s.Put(rc.Key, rc.Val)
		//fmt.Printf("put op is key:%s, val:%s, clientId %d, clientId %d\n", rc.Key, rc.Val, rc.ClientId, rc.SeqId)
		res.Err = err
	} else if rc.CmdType == CmdTypeAppend {
		//fmt.Printf("append op is key:%s, val:%s, clientId %d, clientId %d\n", rc.Key, rc.Val, rc.ClientId, rc.SeqId)
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
	//fmt.Printf("server %d, Get start \n", kv.me)
	index, _, isLeader := kv.rf.Start(Op{CmdType: CmdTypeGet, Key: args.Key})
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

	case <-time.After(TimeOut):
		reply.Err = ErrTimeOut
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
	//fmt.Printf("KVServer %d, PutAppend start, key %s, val %s \n", kv.me, args.Key, args.Value)
	kv.mu.Lock()
	if kv.isRaftCommandDuplicate(args.ClientId, args.SeqId) {
		reply.Err = kv.duplicateReqM[args.ClientId].Rc.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Op{
		CmdType:  getTypeByReq(args.Op) ,
		Key:      args.Key,
		Val:      args.Value,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
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

func getTypeByReq(op string) CmdType {
	switch op {
	case "Put":
		return CmdTypePut
	case "Append":
		return CmdTypeAppend

	default:
		//panic(fmt.Sprintf("err op type %s", op))
		panic(fmt.Sprintf("err op type %s", op))
	}
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
		kv.notifyChs[index] = make(chan RaftCommandResp, 1) // todo bug:
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
	kv.dead = 0
	kv.lastAppliedId = 0

	kv.stateMachine = NewStateMachine()
	kv.notifyChs = make(map[int]chan RaftCommandResp)
	kv.duplicateReqM = make(map[int]LastRaftCommandResp)
	// You may need initialization code here.

	// 启动的时候，需要从snapshot 恢复数据到状态机
	kv.restoreSnapshot(persister.ReadSnapshot())

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
				if message.CommandIndex <= kv.lastAppliedId { // todo bug:   kv.lastAppliedId <= message.CommandIndex
					kv.mu.Unlock()
					continue
				}
				kv.lastAppliedId = message.CommandIndex
				// 用户的操作
				rc := message.Command.(Op)

				if rc.CmdType != CmdTypeGet && kv.isRaftCommandDuplicate(rc.ClientId, rc.SeqId) {
					result = kv.duplicateReqM[rc.ClientId].Rc
				} else {
					//fmt.Printf("who: %d apply: %v op is key:%s, val:%s, clientId: %d, seqId: %d\n",kv.me,rc.CmdType, rc.Key, rc.Val, rc.ClientId, rc.SeqId)
					result = kv.stateMachine.Apply(rc)
					if rc.CmdType != CmdTypeGet {
						kv.duplicateReqM[rc.ClientId] = LastRaftCommandResp{rc.SeqId,result}
					}
				}
				// 判断如果是leader， 则通过 notifyChanle 将结果返回客户端
				if _, isLeader := kv.rf.GetState(); isLeader { // get 一直没等到，返回了也 delete 了chan， 然后这里应用了，chan一直等待
					notifyCh := kv.getNotifyChanel(message.CommandIndex)

					notifyCh <- result
				}

				// 如果需要快照，则创建快照
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					kv.MakeSnapshot(message.CommandIndex)
				}

				kv.mu.Unlock()
			} else if message.SnapshotValid {
				// snapshot 恢复
				kv.mu.Lock()

				kv.restoreSnapshot(message.Snapshot)
				kv.lastAppliedId = message.SnapshotIndex // 因为 SnapshotIndex之前的数据都在 snapshot中都用到了状态机
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
	//if ok && lastInfo.SeqId <= seqId { // todo bug
	if ok &&  seqId <= lastInfo.SeqId { // todo bug
		return true
	}
	return false
}

// 现在状态机应用到了 index 的位置， 对index以下做快照（包括index）。
func (kv *KVServer) MakeSnapshot(index int) {
	buf := new(bytes.Buffer) // 往这里写
	e := labgob.NewEncoder(buf)
	e.Encode(kv.stateMachine)
	e.Encode(kv.duplicateReqM)
	kv.rf.Snapshot(index, buf.Bytes())
	return
}

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	bf := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(bf)
	var stateMachine StateMachine
	var duplicateReqM map[int]LastRaftCommandResp
	err := d.Decode(&stateMachine)
	if err != nil {
		panic( fmt.Sprintf("decode stateMachine err %s", err))
	}
	err = d.Decode(&duplicateReqM)
	if err != nil {
		panic( fmt.Sprintf("decode duplicateReqM err %s", err))
	}

	kv.stateMachine = &stateMachine
	kv.duplicateReqM = duplicateReqM
	return
}