package shardkv

import (
	"bytes"
	"course/labrpc"
	"course/shardctrler"
	"fmt"
	"sync/atomic"
	"time"
)
import "course/raft"
import "sync"
import "course/labgob"

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	notifyChs     map[int]chan RaftCommandResp
	lastAppliedId int

	stateMachines map[int]*StateMachine       // 每个shard 一个 stateMachine
	duplicateReqM map[int]LastRaftCommandResp // 某个clinet 的最后一条消息的结果和 seqId
	dead          int32
	curConfig     shardctrler.Config
	preConfig     shardctrler.Config
	mck           *shardctrler.Clerk
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 调用 *raft.Raft 的 start 方法将 args 的 cmd 通过raft 机制处理
	// 根据raft.start 的返回结果，如果不是leader， 则返回错误 ErrWrongLeader
	// 根据raft.start 的返回结果的index， lock， unlock 通过一个 chan 获取执行结果；
	// 带超时的 select 机制， time.After()
	//fmt.Printf("server %d, Get start \n", kv.me)

	// 判断 key 是本group 的， 如果不是则返回err
	kv.mu.Lock()
	if !kv.isKeyMatch(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(
		RaftCommand{
			RcType: RCClientCmd,
			data:   Op{CmdType: CmdTypeGet, Key: args.Key},
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
		reply.Value = result.Val
		reply.Err = result.Err

	case <-time.After(TimeOut):
		reply.Err = ErrTimeOut
	}
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// Your code here.
	// 判断请求是否重复， 如果重复，则返回err
	// 调用 *raft.Raft 的 start 方法将 args 的 cmd 通过raft 机制处理
	// 根据raft.start 的返回结果，如果不是leader， 则返回错误 ErrWrongLeader
	// 根据raft.start 的返回结果的index， lock， unlock 通过一个 chan 获取执行结果；
	// 带超时的 select 机制， time.After()
	//fmt.Printf("KVServer %d, PutAppend start, key %s, val %s \n", kv.me, args.Key, args.Value)

	kv.mu.Lock()
	if !kv.isKeyMatch(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()


	kv.mu.Lock()
	if kv.isRaftCommandDuplicate(args.ClientId, args.SeqId) {
		reply.Err = kv.duplicateReqM[args.ClientId].Rc.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(
		RaftCommand{
			RcType: RCClientCmd,
			data: Op{
				CmdType:  getTypeByReq(args.Op) ,
				Key:      args.Key,
				Val:      args.Value,
				ClientId: args.ClientId,
				SeqId:    args.SeqId,
			},
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

func getTypeByReq(op string) OpType {
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

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}


func (kv *ShardKV) getNotifyChanel(index int) chan RaftCommandResp {
	if _, ok := kv.notifyChs[index]; !ok {
		kv.notifyChs[index] = make(chan RaftCommandResp, 1) // todo bug:
	}
	return kv.notifyChs[index]
}

func (kv *ShardKV) removeNotifyChanel(index int) {
	delete(kv.notifyChs, index)
}


// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(RaftCommand{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	kv.dead = 0
	kv.lastAppliedId = 0

	for i := 0; i < shardctrler.NShards; i++ {
		kv.stateMachines[i] = NewStateMachine()
	}

	kv.notifyChs = make(map[int]chan RaftCommandResp)
	kv.duplicateReqM = make(map[int]LastRaftCommandResp)
	// You may need initialization code here.

	// 启动的时候，需要从snapshot 恢复数据到状态机
	kv.restoreSnapshot(persister.ReadSnapshot())

	kv.curConfig = shardctrler.DefaultConfig()

	go kv.applyKvRaftTask()

	go kv.getConfigTask()

	go kv.handleConfigChangeTask()

	go kv.shardGcTask()
	return kv
}

// key 
func (kv *ShardKV)isKeyMatch(key string) bool {
	shardId := key2shard(key)
	return kv.curConfig.Shards[shardId] == kv.gid
}

// 判断一个client的 一个seqId 的操作是重复的发送
func (kv *ShardKV) isRaftCommandDuplicate(clientId int, seqId int) bool {
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
func (kv *ShardKV) MakeSnapshot(index int) {
	buf := new(bytes.Buffer) // 往这里写
	e := labgob.NewEncoder(buf)
	e.Encode(kv.stateMachines)
	e.Encode(kv.duplicateReqM)
	kv.rf.Snapshot(index, buf.Bytes())
	return
}

func (kv *ShardKV) restoreSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	bf := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(bf)
	stateMachine := make(map[int]*StateMachine)
	var duplicateReqM map[int]LastRaftCommandResp

	err := d.Decode(&stateMachine)
	if err != nil {
		panic( fmt.Sprintf("decode stateMachine err %s", err))
	}
	err = d.Decode(&duplicateReqM)
	if err != nil {
		panic( fmt.Sprintf("decode duplicateReqM err %s", err))
	}

	kv.stateMachines = stateMachine
	kv.duplicateReqM = duplicateReqM
	return
}


// 处理config 更新
func (kv *ShardKV) ApplyHandleConfig(rc RaftCommand) RaftCommandResp {
	switch rc.RcType {
	case RCConfigChange:
		newConfig := rc.data.(shardctrler.Config)
		return kv.handleConfigChange(newConfig)
	}
}

func (kv *ShardKV) RaftCommandSend(command RaftCommand, reply *RaftCommandResp) {
	index, _, isLeader := kv.rf.Start(command)
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
}

func (kv *ShardKV) handleConfigChange(newConfig shardctrler.Config) (resp RaftCommandResp) {
	/*
	 1 遍历 shards，
	 1 如果newConfig == gid， config != gid ， 则move int； 迁入， cur.shard 原来别人，不是0； 现在过来
	 2 如果newConfig != gid， config == gid， 则move out; 迁出， 现在是我，迁出去，迁出目的不是0
	*/
	if kv.curConfig.Num + 1 != newConfig.Num {
		resp.Err = ErrConfigNum
		return
	}

	for i := 0; i < shardctrler.NShards; i++ {
		if kv.curConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
			// 迁入的shard
			if kv.curConfig.Shards[i] != 0 {
				kv.stateMachines[i].state = ShardMoveIn
			}

		} else if kv.curConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
			// 迁出的shard
			if newConfig.Shards[i] != 0 {
				kv.stateMachines[i].state = ShardMoveOut
			}
		}
	}
	kv.preConfig = kv.curConfig
	kv.curConfig = newConfig
	resp.Err = OK
	return
}

// 处理数据迁移，数据迁移到这里来了, 包括data ，和去重表
func (kv *ShardKV) ApplyHandShardMigration(rc RaftCommand) (resp RaftCommandResp) {
	sData := rc.data.(ShardDataGetResp)
	if sData.ConfigNum != kv.curConfig.Num {
		resp.Err = ErrConfigNum
		return
	}

	// 遍历将每个 stateMachine 的数据拷贝到 kv 本机 stateMachines 中
	for shardId , stateMachine := range sData.Data {
		if kv.stateMachines[shardId].state != ShardMoveIn {
			break
		}
		for k, v := range stateMachine {
			kv.stateMachines[shardId].Mem[k] = v
		}
		kv.stateMachines[shardId].state = ShardGc
	}

	for clientId, msNew := range sData.DuplicateTable {
		msOld, ok := kv.duplicateReqM[clientId]
		if !ok || msOld.SeqId < msNew.SeqId {
			kv.duplicateReqM[clientId] = msNew
		}
	}
	return
}


func (kv *ShardKV) ApplyShardDataGc(rc RaftCommand) (resp RaftCommandResp) {
	req := rc.data.(ShardDataGcReq)
	if req.ConfigNum != kv.curConfig.Num {
		resp.Err = ErrConfigNum
		return
	}

	kv.mu.Lock()
	for shardId, sm := range kv.stateMachines {
		if sm.state == ShardGc {
			sm.state = ShardNormal
		} else if sm.state == ShardMoveOut {
			kv.stateMachines[shardId] = NewStateMachine()
		} else {
			break
		}
	}
	kv.mu.Unlock()
	resp.Err = OK
	return
}


// 获取 gid 对应的哪些 shard; 需要被迁移
func (kv *ShardKV) getShardsByState(state ShardState) map[int][]int {
	gidToShards := make(map[int][]int)

	for shardId, sm := range kv.stateMachines {
		if sm.state == state {
			gid := kv.preConfig.Shards[shardId]
			if gid != 0 {
				gidToShards[gid] = append(gidToShards[gid], shardId)
			}
		}
	}

	return gidToShards
}

// 只从leader 获取数据即可;
func (kv *ShardKV) GetShardsData(args *ShardDataGetArgs, resp *ShardDataGetResp) {
	// 只从leader 获取数据
	if _, isLeader := kv.rf.GetState(); !isLeader {
		resp.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 如果配置不是我们想要的，即， 还未准备好数据迁移，的配置信息。
	if kv.curConfig.Num < args.CofigNum {
		resp.Err = ErrConfigNum
		return
	}

	// 拷贝data
	resp.Data = make(map[int]map[string]string )
	for _, shardId := range args.Shards {
		resp.Data[shardId] = kv.stateMachines[shardId].CopyData()
	}

	// 拷贝duplicaTable
	resp.DuplicateTable = make(map[int]LastRaftCommandResp)
	for k, v := range kv.duplicateReqM {
		resp.DuplicateTable[k] = v
	}

	return
}


// 只从leader 获取数据即可;
func (kv *ShardKV) DeleteShardsData(args *ShardDataGcReq, resp *ShardDataGcResp) {
	// 只从leader 获取数据
	if _, isLeader := kv.rf.GetState(); !isLeader {
		resp.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 如果配置不是我们想要的，即， 还未准备好数据迁移，的配置信息。
	if kv.curConfig.Num < args.ConfigNum {
		resp.Err = ErrConfigNum
		return
	}

	// shard gc
	var raftResp RaftCommandResp
	kv.RaftCommandSend(RaftCommand{RcType: ShardGc}, &raftResp)

	return
}
