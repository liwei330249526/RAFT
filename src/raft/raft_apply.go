package raft

import "sort"

// 应用日志的ticker，接受日志应用信号，将日志应用到状态机，并更新 lastApplyedId
func (rf *Raft)applyTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		rf.applyCond.Wait()
		applyEntries := make([]Entry, 0)

		snapPend := rf.snapPend
		if snapPend {
			// nothing
		} else {
			// 上次应用的 index， 随快照的 lastIncludeIndex更新
			if rf.lastApplyedId < rf.logs.lastIncludeTerm { //todo: recode bug1
				rf.lastApplyedId = rf.logs.lastIncludeIndex
			}

			// 如果leader 发来的commited index 大于本地日志最大值
			end := rf.committedId
			if rf.logs.size()-1 < end {
				end = rf.logs.size()-1
			}
			for i := rf.lastApplyedId+1; i <= end; i++ { // 注意从 lastApplyedId+1 复制； 其实再append的时候已经解决了
				applyEntries = append(applyEntries, rf.logs.at(i))
			}
		}

		rf.mu.Unlock()


		if snapPend {
			// 发送快照
			snap := ApplyMsg{
				SnapshotValid : true,
				Snapshot      : rf.logs.snapshot,
				SnapshotTerm  : rf.logs.lastIncludeTerm,
				SnapshotIndex : rf.logs.lastIncludeIndex, // 这俩搞反向了
			}
			rf.applyCh <- snap
			LOG(rf.me, rf.curTerm, DSnap, "appCh snap %s", snap.String())

		} else {
			for i := 0; i < len(applyEntries); i++ {
				am := ApplyMsg{
					CommandValid: applyEntries[i].ValidCmd,
					Command:applyEntries[i].Cmd,
					//CommandIndex:applyEntries[i].Id, // todo: id 是哪个
					CommandIndex: rf.lastApplyedId + 1 + i,
				}
				//LOG(rf.me, rf.curTerm, DDebug, "msg %v %v, %v", rf.lastApplyedId, am.Command, am.CommandIndex)
				rf.applyCh <- am
			}
		}

		rf.mu.Lock()
		if snapPend {
			LOG(rf.me, rf.curTerm, DSnap, "Apply snapshot for [%d, %d]",0, rf.logs.lastIncludeIndex)
			rf.lastApplyedId = rf.logs.lastIncludeIndex
			if rf.committedId < rf.lastApplyedId {
				rf.committedId = rf.lastApplyedId
			}

			rf.snapPend = false
		} else {
			LOG(rf.me, rf.curTerm, DApply, "Apply log for [%d, %d]", rf.lastApplyedId+1, rf.lastApplyedId+len(applyEntries))
			rf.lastApplyedId = rf.lastApplyedId + len(applyEntries)
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) getMaxMajorIndex() int {
	//复制一份 matchIndex
	ml := make([]int, len(rf.match))
	copy(ml, rf.match)

	//对matchIndex 排序，获取中位数
	sort.Ints(ml)
	//mxId := len(ml)/2 -1
	mxId := (len(ml)-1) / 2

	LOG(rf.me, rf.curTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", ml, mxId, ml[mxId])


	//返回该中位数位置的 index
	return ml[mxId]
}