package shardctrler

import "time"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// 返回值敞亮
const (
	OK             = "OK" // 无错误
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
	ErrKeyNotExist = "ErrKeyNotExist"
	ErrSeqDuplica = "ErrSeqDuplica"
)

type CmdType uint32
const (
	CmdTypeJoin CmdType = iota
	CmdTypeLeave
	CmdTypeMove
	CmdTypeQuery
)

const (
	TimeOut = time.Millisecond * 500
)


// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

type Err string

type JoinArgs struct {
	ClientId int
	SeqId int
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	Err         Err
}

type LeaveArgs struct {
	ClientId int
	SeqId int
	GIDs []int
}

type LeaveReply struct {
	Err         Err
}

type MoveArgs struct {
	ClientId int
	SeqId int
	Shard int
	GID   int
}

type MoveReply struct {
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	Err         Err
	Config      Config
}


type Op struct { // todo: 字段定义
	CmdType CmdType
	Servers map[int][]string // for join new GID -> servers mapping
	GIDs []int  // for leave
	Shard int   // for move
	GID   int   // for move

	Num int     // for query
	ClientId int
	SeqId int
}

type RaftCommandResp struct { // todo : 字段定义
	Config      Config // for query
	Err Err // 有可能有错误，或nil
}

type LastRaftCommandResp struct {
	SeqId int
	Rc    RaftCommandResp
}