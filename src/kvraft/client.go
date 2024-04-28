package kvraft

import "course/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int // 能获取数据的是leader, 获取数据后保存在这里
	clientId int // put 操作需要一个clientId，指代是哪个客户端写的， todo: liwei 这俩怎么初始化, 随机数
	seqId int // put 操作应该需要序列号 ， 初始化为0
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = int(nrand())
	ck.seqId = 0
	return ck

}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	// 获取key 对应的val
	// 从leaderId 开始远程调用 call
	// 如果获取失败，则尝试下一个节点，轮询获取, 问题，如果一直获取不到怎么办?
	// 获取失败的3个条件， !ok, 不是leader， server 返回超时
	for {
		args := GetArgs{
			Key:key,
		}
		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.leaderId = (ck.leaderId+1) % len(ck.servers)
			continue
		}
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// 获取写 key val
	// 从leaderId 开始远程调用 call
	// 如果获取失败，则尝试下一个节点，轮询调用
	// 从leaderId开始远程调用，发送req， 成功后，设置 seqId++
	for {
		args := PutAppendArgs{
			Key:key,
			Value: value,
			Op: op,
			ClientId: ck.clientId,
			SeqId: ck.seqId,
		}
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.leaderId = (ck.leaderId+1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}
// 写key val
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// 追加 key val
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
