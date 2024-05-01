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
	ErrKeyNotExist = "ErrKeyNotExist"
	ErrSeqDuplica = "ErrSeqDuplica"
)

const (
	TimeOut = time.Millisecond * 500
	GetConfigInterval = time.Millisecond * 100
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
type CmdType uint32
const (
	CmdTypeGet CmdType = iota
	CmdTypePut
	CmdTypeAppend
)
type Op struct { // todo: 字段定义
	CmdType CmdType
	Key string
	Val string
	ClientId int
	SeqId int
}

type RaftCommandResp struct { // todo : 字段定义
	Val string
	Err Err // 有可能有错误，或nil
}

type LastRaftCommandResp struct {
	SeqId int
	Rc    RaftCommandResp
}