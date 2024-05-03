package shardkv

import "time"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"


	ErrTimeOut     = "ErrTimeOut"
	ErrConfigNum   = "ErrConfigNum"
	ErrKeyNotExist = "ErrKeyNotExist"
	ErrSeqDuplica = "ErrSeqDuplica"
)

const (
	TimeOut = time.Millisecond * 500
	GetConfigInterval = time.Millisecond * 100
	handleConfigChangeInterval = time.Millisecond * 50
)


type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ClientId int
	SeqId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// cmd 类型
type OpType uint32
const (
	CmdTypeGet OpType = iota
	CmdTypePut
	CmdTypeAppend
)

type RaftCommandType uint32
const (
	ClientCmd RaftCommandType = iota
	ConfigChange
	ShardMigration
)

type ShardState uint32

const (
	ShardNormal ShardState = iota
	ShardMoveIn
	ShardMoveOut
	ShardGc
)

type Op struct { // todo: 字段定义
	CmdType  OpType
	Key      string
	Val      string
	ClientId int
	SeqId    int
}

type RaftCommand struct {
	 RcType RaftCommandType
	 data interface{} // Op 或 config
}

type RaftCommandResp struct { // todo : 字段定义
	Val string
	Err Err // 有可能有错误，或nil
}

type LastRaftCommandResp struct {
	SeqId int
	Rc    RaftCommandResp
}

type ShardDataGetArgs struct {
	CofigNum int
	Shards []int
}

type ShardDataGetResp struct {
	Err Err
	ConfigNum int
	Data map[int]map[string]string // 每个shard 的数据
	DuplicateTable map[int]LastRaftCommandResp // 每个shard 的去重表信息
}