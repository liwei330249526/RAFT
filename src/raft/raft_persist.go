package raft

import (
	"bytes"
	"course/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (PartC).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.curTerm)
	e.Encode(rf.votedFor)

	rf.logs.persist(e)
	//e.Encode(rf.logs)
	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (PartC).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var curTerm int
	var votedFor int
	//var logs []Entry
	if d.Decode(&curTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
		rf.logs.readPersist(d) != nil {
		//d.Decode(&logs) != nil{
	  LOG(rf.me, rf.curTerm, DPersist, "Decode term, votedFor logs err")
	} else {
	  rf.curTerm = curTerm
	  rf.votedFor = votedFor
	  //rf.logs= logs
	}
	return
}