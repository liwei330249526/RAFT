package shardkv

import "fmt"

// 基于内存的kv ， 可以转化为基于磁盘的kv
type StateMachine struct {
	Mem map[string]string
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		Mem: make(map[string]string),
	}
}

func (s *StateMachine)Get(key string) (string, Err) {
	if val, ok := s.Mem[key]; ok {
		return val, OK
	}
	return "", ErrNoKey
}
func (s *StateMachine)Put(key string, val string) Err {
	s.Mem[key] = val
	return OK
}

func (s *StateMachine)Append(key string, val string) Err {
	s.Mem[key] += val
	return OK
}

// 状态机应用日志, todo: 可改为cckv 的单机存储引擎
func (s *StateMachine)Apply(rc Op) RaftCommandResp {
	res := RaftCommandResp{}
	if rc.CmdType == CmdTypeGet {
		val, err := s.Get(rc.Key)
		fmt.Printf("get op is key:%s, val:%s, clientId: %d, seqId: %d\n", rc.Key, val, rc.ClientId, rc.SeqId)
		res.Val = val
		res.Err = err
	} else if rc.CmdType == CmdTypePut {
		err := s.Put(rc.Key, rc.Val)
		fmt.Printf("put op is key:%s, val:%s, clientId %d, clientId %d\n", rc.Key, rc.Val, rc.ClientId, rc.SeqId)
		res.Err = err
	} else if rc.CmdType == CmdTypeAppend {
		fmt.Printf("append op is key:%s, val:%s, clientId %d, clientId %d\n", rc.Key, rc.Val, rc.ClientId, rc.SeqId)
		err := s.Append(rc.Key, rc.Val)
		res.Err = err
	}
	return res
}