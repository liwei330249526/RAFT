package shardkv

import (
	"sync"
	"time"
)

// 定时task， 接受raft 的应用 cmd，将cmd 应用到 stateMachine
func (kv *ShardKV)applyKvRaftTask() {
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
				rc := message.Command.(RaftCommand)
				if rc.RcType == RCClientCmd {
					op := rc.data.(Op)
					if op.CmdType != CmdTypeGet && kv.isRaftCommandDuplicate(op.ClientId, op.SeqId) {
						result = kv.duplicateReqM[op.ClientId].Rc
					} else {
						//fmt.Printf("who: %d apply: %v op is key:%s, val:%s, clientId: %d, seqId: %d\n",kv.me,rc.CmdType, rc.Key, rc.Val, rc.ClientId, rc.SeqId)
						shardId := key2shard(op.Key) // shard
						result = kv.stateMachines[shardId].Apply(op)
						if op.CmdType != CmdTypeGet {
							kv.duplicateReqM[op.ClientId] = LastRaftCommandResp{op.SeqId,result}
						}
					}
				} else if rc.RcType == RCConfigChange {
					result = kv.ApplyHandleConfig(rc)
				} else if rc.RcType == RCShardMigration {
					result = kv.ApplyHandShardMigration(rc)
				} else if rc.RcType == RCShardDataGc {
					result = kv.ApplyShardDataGc(rc)
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

func (kv *ShardKV) getConfigTask() {
	// 这里 leader 节点都会各自查询config 到自己节点；
	// 应该改为， 查到新的后，交给raft 模块处理， 日志应用后，说明3个 replica 都共识了 config 变更
	// 日志应用时，每个节点进行 config 变更的实施，config变更，data变更。
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			//config := kv.mck.Query(-1)
			//kv.curConfig = config

			needGet := true
			kv.mu.Lock()
			// 如果有 shard 再 迁移， 则本次不做迁移
			for _, stateMachine := range kv.stateMachines {
				if stateMachine.state != ShardNormal {
					needGet = false
					break
				}
			}
			kv.mu.Unlock()

			if !needGet {
				continue
			}

			num := kv.curConfig.Num
			config := kv.mck.Query(num+1) // leader 节点查询最新到最新配置

			if config.Num == num+1 {
				resp := RaftCommandResp{}
				kv.RaftCommandSend(
					RaftCommand{ // 将最新配置信息通过raft 模块做多副本共识
						RcType: RCConfigChange,
						data:   config,
					},
					&resp)
			}

			time.Sleep(GetConfigInterval)
		}

	}
}

func (kv *ShardKV)handleConfigChangeTask() {
	// 获取所有 move in 的 shard， 做处理
	// 获取所有 move out 的shard ， 做处理
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			gidToShards := kv.getShardsByState(ShardMoveIn)
			// 迁移数据进来
			// 对每个gid 并行发送请求
			var w sync.WaitGroup
			for gid, shards := range gidToShards {
				w.Add(1)
				// 遍历group 的每个节点，从leader 获取 shards 数据
				// req, resp
				go func(configNum int, shards []int, servers []string) {
					defer w.Done()
					req := ShardDataGetArgs {
						CofigNum: configNum,
						Shards: shards,
					}
					// 从原来的group 对应的shards 拿到对应的数据
					for _, server := range servers {
						resp := ShardDataGetResp{}
						cl := kv.make_end(server)
						// rpc 调用
						ok := cl.Call("ShardKV.GetShardsData", &req, &resp)
						if ok && resp.Err == OK {
							// 获取了对应的数据，执行 shard 迁移
							// 这个 shard 迁移的命令通过raft 共识后， 可以在apply 协程中处理 shard 迁移
							var raftResp RaftCommandResp
							kv.RaftCommandSend(RaftCommand{RcType: RCShardMigration, data: resp}, &raftResp)
						}
					}


				}(kv.preConfig.Num, shards, kv.preConfig.Groups[gid])
			}

			w.Wait()
			kv.mu.Unlock()
			time.Sleep(handleConfigChangeInterval)
		}
	}
}

func (kv *ShardKV)shardGcTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			var wg sync.WaitGroup // 并发

			gidToShards := kv.getShardsByState(ShardGc)
			for gid, shards := range gidToShards {
				wg.Add(1)
				go func(configNum int, shards []int, servers []string) {
					defer wg.Done()
					req := RCShardDataGc{
						CofigNum: configNum,
						Shards: shards,
					}
					// 从原来的group 对应的shards 拿到对应的数据
					for _, server := range servers {
						resp := ShardDataGcResp{}
						cl := kv.make_end(server)
						// rpc 调用
						ok := cl.Call("ShardKV.DeleteShardsData", &req, &resp)
						if ok && resp.Err == OK {
							// 获取了对应的数据，执行 shard 迁移
							// 这个 shard 迁移的命令通过raft 共识后， 可以在apply 协程中处理 shard 迁移
							var raftResp RaftCommandResp
							kv.RaftCommandSend(RaftCommand{RcType: RCShardDataGc}, &raftResp)
						}
					}

				}(kv.preConfig.Num, shards, kv.preConfig.Groups[gid])
			}

			wg.Wait()
			kv.mu.Unlock()
			time.Sleep(ShardGcInterval)
		}
	}
}
