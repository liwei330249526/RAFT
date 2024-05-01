package shardctrler

import (
	"fmt"
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
	//fmt.Println("StateMachine Query num, config", num, s.configs)
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
		Num: len(s.configs),
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
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shardId, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shardId)
	}

	for {
		maxId := getMaxShardGid(gidToShards)
		minId := getMinShardGid(gidToShards)
		// 退出条件， 除了gid 为0 的，最终就是要gid为0的里面shard数为0；  其他的group 的做到负载均衡
		if maxId != 0 && len(gidToShards[maxId]) - len(gidToShards[minId]) <= 1 {
			break
		}

		gidToShards[minId] = append(gidToShards[minId], gidToShards[maxId][0])
		gidToShards[maxId] = gidToShards[maxId][1:]

	}

	var newShards [NShards]int
	for gid, shardIds := range gidToShards {
		for _, shardId := range shardIds {
			if gid == -1 {
				panic("gid = -1")
			}
			newShards[shardId] = gid
		}
	}

	newConfig.Shards = newShards

	s.configs = append(s.configs, newConfig)

	return OK
}

// 找 shard 最多的 gid
func getMaxShardGid(gidToShards map[int][]int) int {
	// 这里初始化的时候，所有的shard 的groupId 都为0， 因为shard是为10的数组，默认val 为0
	shardIds, ok := gidToShards[0]
	if ok && len(shardIds) > 0 {
		return 0
	}

	gids := make([]int, 0)

	for gid, _ := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	mGid := -1
	mCount := -1
	for _, gid := range gids{
		// 最小的gid 组是接受 shard 的组，不能为 0 组
		if gid != 0 && mCount < len(gidToShards[gid]){
			mCount = len(gidToShards[gid])
			mGid = gid
		}
	}
	return mGid
}

// 找 shard 最少的 gid
func getMinShardGid(gidToShards map[int][]int) int {
	gids := make([]int, 0)
	// 如果有 两个组的shard 竖向一样，则遍历map时，拿到的结果可能是不一样的
	// 所以要对gid排序，遍历map时，以一个固定的顺序遍历，就能取到一个固定的 最大gid
	for gid, _ := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	mGid := -1
	mCount := math.MaxInt32
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
	// 1 遍历gides， 再groups 中删除
	// 1 获得 gids 对应的所有的shard， 并在 gidToShards中删除 gid
	// 2 通过gidToShards 选一个最少shard 的gid， 将1 中的shard 给到最少shard 的gid
	// 3 最后 1 中的shard 为空
	// 4 通过gidToShard 构造新的shards， 加入newConfig
	oldConfig := s.configs[len(s.configs)-1]

	newConfig := Config{
		Num: len(s.configs),
		Shards: oldConfig.Shards,
		Groups: copyMap(oldConfig.Groups) ,
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
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shardId, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shardId)
	}

	leavedGidShard := make([]int, 0)
	for _, gid := range gids {
		delete(newConfig.Groups, gid)
		if shards, ok := gidToShards[gid]; ok {
			leavedGidShard = append(leavedGidShard, shards...)
			delete(gidToShards, gid)
		}
	}


	var newShards [NShards]int

	//if len(leavedGidShard) != 0 { // todo bug:
	if len(newConfig.Groups) != 0 { // 如果没有group 了，则 leavedGidShard 也不用负载均衡了，直接删掉即可
		for _, shard := range leavedGidShard {
			minGid := getMinShardGid(gidToShards)
			gidToShards[minGid] = append(gidToShards[minGid], shard)
		}
		for gid, shardIds := range gidToShards {
			for _, shardId := range shardIds {
				if gid == -1 {
					panic(fmt.Sprintf("gid = -1, %d, %d, newConfig.Groups:%v; gidToShards %v, gids %v",
						shardId, gid, newConfig.Groups, gidToShards, gids))
				}
				newShards[shardId] = gid
			}
		}
	}

	newConfig.Shards = newShards

	s.configs = append(s.configs, newConfig)

	return OK
}

// 将这个shard 的 group 改为 gid
func (s *StateMachine)Move(shard int, gid int) Err {
	//s.Mem[key] += val
	oldConfig := s.configs[len(s.configs)-1]

	newConfig := Config{
		Num: len(s.configs),
		Shards: oldConfig.Shards, // 如果复制数组，可以直接通过赋值进行复制；
		Groups: copyMap(oldConfig.Groups) ,
	}
	if gid == -1 {
		panic(fmt.Sprintf("gid = -1, %d, %d", shard, gid) )
	}
	newConfig.Shards[shard] = gid
	s.configs = append(s.configs, newConfig)

	return OK
}

// 状态机应用日志,
func (s *StateMachine)Apply(rc Op) RaftCommandResp {
	res := RaftCommandResp{}
	if rc.CmdType == CmdTypeJoin {
		err := s.Join(rc.Servers)
		//fmt.Printf("get op is key:%s, val:%s, clientId: %d, seqId: %d\n", rc.Key, val, rc.ClientId, rc.SeqId)
		res.Err = err
	} else if rc.CmdType == CmdTypeLeave {
		err := s.Leave(rc.GIDs)
		//fmt.Printf("put op is key:%s, val:%s, clientId %d, clientId %d\n", rc.Key, rc.Val, rc.ClientId, rc.SeqId)
		res.Err = err
	} else if rc.CmdType == CmdTypeMove {
		//fmt.Printf("append op is key:%s, val:%s, clientId %d, clientId %d\n", rc.Key, rc.Val, rc.ClientId, rc.SeqId)
		err := s.Move(rc.Shard, rc.GID)
		res.Err = err
	} else if rc.CmdType == CmdTypeQuery {
		//fmt.Printf("append op is key:%s, val:%s, clientId %d, clientId %d\n", rc.Key, rc.Val, rc.ClientId, rc.SeqId)
		config := s.Query(rc.Num)
		res.Config = config
		res.Err = OK
	}
	return res
}
