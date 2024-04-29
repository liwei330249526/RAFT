package kvraft

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
	return "", ErrKeyNotExist
}
func (s *StateMachine)Put(key string, val string) Err {
	s.Mem[key] = val
	return OK
}

func (s *StateMachine)Append(key string, val string) Err {
	s.Mem[key] += val
	return OK
}
