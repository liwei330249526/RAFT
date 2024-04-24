package raft

import (
	"course/labgob"
	"fmt"
)

type RaftLog struct {
	lastIncludeIndex int // 快照的最后一条日志索引
	lastIncludeTerm  int // 快照的最后一条日志任期
	snapshot []byte     // 快照
	tailLog []Entry      // 日志
}

func NewLog(lastIncludeIndex int, lastIncludeTerm int, snapShort []byte, tailLog []Entry) *RaftLog {
	rl := &RaftLog{
		lastIncludeIndex: lastIncludeIndex,
		lastIncludeTerm: lastIncludeTerm,
		snapshot: snapShort,
	}

	rl.tailLog = append(rl.tailLog, Entry{Term:lastIncludeTerm})
	rl.tailLog = append(rl.tailLog, append([]Entry{}, tailLog...)...)
	return rl
}

// 从 d 中解码出raft 信息
func(rl *RaftLog) readPersist(d *labgob.LabDecoder) error { // 这里少一个点，就不行了, 必须是指针类型实现
	lastIncludeIndex := 0
	if err := d.Decode(&lastIncludeIndex); err != nil {
		return fmt.Errorf("decode lastIncludeIndex err %s", err)
	}
	rl.lastIncludeIndex = lastIncludeIndex

	lastIncludeTerm := 0
	if err := d.Decode(&lastIncludeTerm); err != nil {
		return fmt.Errorf("decode lastIncludeTerm err %s", err)
	}
	rl.lastIncludeTerm = lastIncludeTerm

	var logs []Entry
	if err := d.Decode(&logs); err != nil {
		return fmt.Errorf("decode tailLog err %s", err)
	}
	rl.tailLog = logs

	return nil
}

// 将raftlog 编码到 []byte
func (rl *RaftLog)persist(e *labgob.LabEncoder) {
	e.Encode(rl.lastIncludeIndex)
	e.Encode(rl.lastIncludeTerm)
	e.Encode(rl.tailLog)
	return
}

// 下标转换
func (rl *RaftLog)size() int {
	return rl.lastIncludeIndex + len(rl.tailLog)
}

// 将全局id 转换为局部id
func(rl *RaftLog)idx(logicId int) int {
	if logicId < rl.lastIncludeIndex || logicId >= rl.size() { // logicId == lastIncludeIndex 是可以的，可计算出0
		panic(fmt.Sprintf("logicId %d beyond range [%d - %d]\n", logicId, rl.lastIncludeIndex+1, rl.size()-1))
	}

	return logicId - rl.lastIncludeIndex
}

// 返回全局id处的日志
func(rl *RaftLog)at(logicId int) Entry {
	id := rl.idx(logicId)
	return rl.tailLog[id]
}

// 返回最新日志的id 和任期
func (rl *RaftLog)last() (int, int) {
	id := rl.size()-1
	term := rl.tailLog[len(rl.tailLog)-1].Term
	return id, term
}

func (rl *RaftLog)append(cmd Entry) {
	rl.tailLog = append(rl.tailLog, cmd)
}

func (rl *RaftLog) appendFrom(preId int, logs []Entry) {
	id := rl.idx(preId)
	rl.tailLog = append(rl.tailLog[:id+1], append([]Entry{}, logs...)...)
	return
}

func (rl *RaftLog) tailsLogs(start int) []Entry {
	if start >= rl.size() {
		return nil
	}
	id := rl.idx(start)
	return rl.tailLog[id:]
}
/*
          3
 10 11 12 13 14 15

 index =13

localId = 3
*/
func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	//	lastIncludeIndex， lastIncludeTerm 赋值
	//snapshot 赋值
	//newLog 赋值[0] ， lastIncludeTerm
	//newLog 赋值[1:]， idx+1: 截断日志
	//persist()
	localId := rl.idx(index)
	rl.lastIncludeIndex = index
	// rl.lastIncludeTerm = rl.at(index).Term // 由于上面已经更新了lastIncludeIndex， 所以这里不能再次用 at 函数
	rl.lastIncludeTerm = rl.tailLog[localId].Term   // todo : recode bug3, 所有lastIncludeTerm 都为0 了
	rl.snapshot = snapshot

	newLog := make([]Entry, 0)
	newLog = append(newLog, Entry{Term: rl.lastIncludeTerm})
	newLog = append(newLog, rl.tailLog[localId+1:]...)
	rl.tailLog = newLog
}

func (rl *RaftLog) installSnapshot(index int, term int, snapshot []byte) {
	rl.lastIncludeIndex = index
	rl.lastIncludeTerm = term
	rl.snapshot = snapshot

	newLog := make([]Entry, 0)
	newLog = append(newLog, Entry{Term:term})
	rl.tailLog = newLog
	return
}