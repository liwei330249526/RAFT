package raft

import "sort"

// 应用日志的ticker，接受日志应用信号，将日志应用到状态机，并更新 lastApplyedId
func (rf *Raft)applyTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		rf.applyCond.Wait()
		applyEntries := make([]Entry, 0)
		for i := rf.lastApplyedId+1; i <= rf.committedId; i++ { // 注意从 lastApplyedId+1 复制
			applyEntries = append(applyEntries, rf.logs.at(i))
		}
		rf.mu.Unlock()

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

		rf.mu.Lock()
		LOG(rf.me, rf.curTerm, DApply, "Apply log for [%d, %d]", rf.lastApplyedId+1, rf.lastApplyedId+len(applyEntries))
		rf.lastApplyedId = rf.lastApplyedId + len(applyEntries)
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