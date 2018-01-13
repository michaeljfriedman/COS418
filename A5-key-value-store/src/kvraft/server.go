package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//------------------------------------------------------------------------------

// Ops

// Constants for Op types
const (
	Get    = "Get"
	Put    = "Put"
	Append = "Append"
)

//
// Unique IDs for Ops. 2-tuple (Client ID, Client Op ID)
//
type OpId struct {
	ClientId   int
	ClientOpId int
}

//
// An Op represents a Get, Put, or Append operation
//
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Type  string  // Get, Put, or Append
	Id    OpId
}

//------------------------------------------------------------------------------

// RaftKV data type
type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// For each unique op requested by a client, this server will add a mapping
	// here. When a value is received on the channel, indicates that this
	// server should reply to the client with success (i.e. op was applied).
	// The value on the channel is the value for a Get, or empty for Put/Append.
	appliedChs map[OpId](chan string)
}

//------------------------------------------------------------------------------

// Get/Put/Append ops

//
// Starts a Get/Put/Append operation. This will either return false for
// `success`, along with an err; or it will return true for `success`
// with no err, and the value received from a Get op.
//
// `success` indicates whether this server should reply to the client
// that the op was successful, or whether the client should retry the op.
//
func (kv *RaftKV) doOp(key string, value string, type string, opId OpId)
(success bool, err Err, value string) {
	// TODO: Implement doOp()
}

//
// Executes a Get operation. Replies to the client with success and the
// value of the Get once the operation has been applied, or with failure/err
// if the client should retry on a different server.
//
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

//
// Executes a Put/Append operation. Analogous to Get, but without returning
// a value upon success.
//
func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

//------------------------------------------------------------------------------

// Applying operations

//
// Continuously reads from the applyCh for new ops to apply. When an op comes
// in, applies it, and sends a message to the pending doOp() via the
// corresponding chan in appliedChs, if there is one. The message will contain
// either the value, if it's a Get op, or the empty string, if it's a
// Put/Append.
//
func (kv *RaftKV) applyOps() {
	// TODO: Implement applyOps
}

//------------------------------------------------------------------------------

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.appliedChs = make(map[OpId](chan string))

	// Start waiting for applied ops
	go applyOps()

	return kv
}
