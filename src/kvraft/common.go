package kvraft

import "time"

const (
	OK             = "OK" // 无错误
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
	ErrKeyNotExist = "ErrKeyNotExist"
	ErrSeqDuplica = "ErrSeqDuplica"
)


var RaftTypeGet CmdType = "RaftTypeGet"
var RaftTypePut CmdType = "RaftTypePut"
var RaftTypeAppend CmdType = "RaftTypeAppend"

const (
	TimeOut = time.Millisecond * 500
)


type Err string

// Put or Append
type PutAppendArgs struct {
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

type CmdType string


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
	rc RaftCommandResp
}
