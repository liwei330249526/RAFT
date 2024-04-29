package shardctrler

import (
	"math"
	"sort"
)

// 基于内存的kv ， 可以转化为基于磁盘的kv
type StateMachine struct {
	configs []Config
}

func NewStateMachine() *StateMachine {
	sm := &StateMachine{}
	sm.configs = make([]Config, 1)
	sm.configs[0] = DefaultConfig()
	return sm
}

func DefaultConfig() Config {
	cfg := Config{
		Groups: make(map[int][]string),
	}
	return cfg
}

// 查询一个配置
func (s *StateMachine)Query(num int) Config {
	if num < 0 || num >= len(s.configs) {
		return s.configs[len(s.configs)-1]
	}
	return s.configs[num]
}

// 向状态机找那个加入 group，  即某个group 有那几个 sever； [gid][s1,s2,s3]
func (s *StateMachine)Join(groups map[int][]string) Err {
	// newconfig， 注意copy map
	// groups 加入到 newconfig group 中， 如果不存在则加入
	// 获取所有 gid，map[gid][]shardId , 数组map；
	// 遍历shards ， 加入map[gid][]shardId ， 计算gid shard1 shard2 shard3
	// 找到最少的 gid， 最多的 gid， 将最多的gid 给到最少的gid 一个shard
	// 根据 map[gid][]shardId  计算新的 shards 加入到 newconfig中

	oldConfig := s.configs[len(s.configs)-1]

	newConfig := Config{
		Num: oldConfig.Num,
		Shards: oldConfig.Shards,
		Groups: copyMap(oldConfig.Groups) ,
	}

	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newConfig.Groups[gid] = append([]string{}, servers...)
		}
	}

	/*
	shardId      gid
	1             1
	2             1
	3             2
	4             2
	5             2
	6             3

	--->

	gid      shardIds
	1          1,2
	2          3,4,5
	3          6
	gidToShards

	*/
	gidToShards := make(map[int][]int)
	for shardId, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shardId)
	}

	for {
		maxId := getMaxShardGid(gidToShards)
		minId := getMinShardGid(gidToShards)
		if maxId - minId <= 1 {
			break
		}

		gidToShards[minId] = append(gidToShards[minId], gidToShards[maxId][0])
		gidToShards[maxId] = gidToShards[maxId][1:]

	}



	var newShards [NShards]int
	for gid, shardIds := range gidToShards {
		for _, shardId := range shardIds {
			newShards[shardId] = gid
		}
	}

	newConfig.Shards = newShards

	s.configs = append(s.configs, newConfig)

	return OK
}

// 找 shard 最多的 gid
func getMinShardGid(gidToShards map[int][]int) int {
	gids := make([]int, 0)

	for gid, _ := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	mGid := -1
	mCount := -1
	for _, gid := range gids{
		if gid != 0 && mCount < len(gidToShards[gid]){
			mCount = len(gidToShards[gid])
			mGid = gid
		}
	}
	return mGid
}

// 找 shard 最少的 gid
func getMaxShardGid(gidToShards map[int][]int) int {
	gids := make([]int, 0)

	for gid, _ := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	mGid := -1
	mCount := math.MaxInt
	for _, gid := range gids{
		if gid != 0 && mCount > len(gidToShards[gid]){
			mCount = len(gidToShards[gid])
			mGid = gid
		}
	}
	return mGid
}

func copyMap(groups map[int][]string) map[int][]string {
	newM := make(map[int][]string)

	for gid, servers := range groups {
		newM[gid] = append([]string{}, servers...)
	}
	return newM
}

// 这些gid 要leave 了
func (s *StateMachine)Leave(gids []int) Err {
	//s.Mem[key] += val
	return OK
}

// 将这个shard 的 group 改为 gid
func (s *StateMachine)Move(shard int, gid int) Err {
	//s.Mem[key] += val
	return OK
}

// 状态机应用日志, todo: 可改为cckv 的单机存储引擎
func (s *StateMachine)Apply(rc Op) RaftCommandResp {
	res := RaftCommandResp{}
	//if rc.CmdType == CmdTypeGet {
	//	val, err := s.Get(rc.Key)
	//	//fmt.Printf("get op is key:%s, val:%s, clientId: %d, seqId: %d\n", rc.Key, val, rc.ClientId, rc.SeqId)
	//	res.Val = val
	//	res.Err = err
	//} else if rc.CmdType == CmdTypePut {
	//	err := s.Put(rc.Key, rc.Val)
	//	//fmt.Printf("put op is key:%s, val:%s, clientId %d, clientId %d\n", rc.Key, rc.Val, rc.ClientId, rc.SeqId)
	//	res.Err = err
	//} else if rc.CmdType == CmdTypeAppend {
	//	//fmt.Printf("append op is key:%s, val:%s, clientId %d, clientId %d\n", rc.Key, rc.Val, rc.ClientId, rc.SeqId)
	//	err := s.Append(rc.Key, rc.Val)
	//	res.Err = err
	//}
	return res
}
