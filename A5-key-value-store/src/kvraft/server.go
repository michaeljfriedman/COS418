package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

//------------------------------------------------------------------------------

// Debugging tools

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//------------------------------------------------------------------------------

// Ops

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

//
// Returns a string form for an op. Has the format:
//   (1, 1): Put("key1", "value1")
//     ^id
//
func (op Op) toString() string {
	if op.Type == Get {
		return fmt.Sprintf("(%v, %v): %v(%v)", op.Id.ClientId, op.Id.ClientOpId,
			op.Type, op.Key)
	} else {
		return fmt.Sprintf("(%v, %v): %v(%v, %v)", op.Id.ClientId, op.Id.ClientOpId,
			op.Type, op.Key, op.Value)
	}
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

	// This server's key-value mapping
	kvMap map[string]string

	// For each unique op requested by a client, this server will add a mapping
	// here. When a value is received on the channel, it indicates that this
	// server should reply to the client with success (i.e. op was applied).
	// The value on the channel is the value for a Get, or empty for Put/Append.
	appliedChs map[OpId](chan string)
}

//------------------------------------------------------------------------------

// Get/Put/Append ops

// Timeout for an operation to commit/be applied, in milliseconds.
const OpTimeout = 1000

//
// Checks if an op is already pending in the raft system, by its opId.
// Returns boolean indicating whether it is.
//
// **Must be called while locked. Assumes the caller has locked before
// calling.**
//
func (kv *RaftKV) isDuplicate(opId OpId) bool {
	// TODO: Implement isDuplicate()
	return false
}

//
// Starts a Get/Put/Append operation. This will either return false for
// `success`, along with an err; or it will return true for `success` with
// OK for err, and the value received from a Get op `value` (if not a Get
// op, `value` will be empty).
//
// `success` indicates whether this server should reply to the client
// that the op was successful, or whether the client should retry the op.
//
// (In case it's not clear, the param `t` is the type Get/Put/Append.
// Can't use the word "type" since this is a keyword in Go.)
func (kv *RaftKV) doOp(key string, putValue string, t string, opId OpId) (success bool, err Err, value string) {
	// Make a new op
	op := Op{key, putValue, t, opId}
	DPrintf("%v got op %v\n", kv.me, op.toString())

	// First, check if this op is a retry of an old op. Do not call
	// Start() for a duplicate op.
	go func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		if !kv.isDuplicate(opId) {
			// Send this op to raft for consensus
			_, _, isLeader := kv.rf.Start(op)

			// Can't proceed if I'm not the leader
			if !isLeader {
				success = false
				err = ErrWrongLeader
				value = ""

				DPrintf("%v not proceeding op %v. Not leader\n", kv.me, op.toString())
				return
			}
		}

		// Set up channel with applyOps() for a result from this op
		ch := make(chan string, 1)
		kv.appliedChs[opId] = ch
	}()

	// Wait for result from applyOps(), or time out
	timer := time.NewTimer(time.Millisecond * time.Duration(OpTimeout))
	DPrintf("%v started op %v. Waiting for completion...\n", kv.me, op.toString())
	select {
	case value = <-kv.appliedChs[opId]:
		// Op was successfully applied
		success = true
		err = OK
		DPrintf("%v is done with op %v\n", kv.me, op.toString())
	case <-timer.C:
		// Timed out
		success = false
		err = ErrTimeout
		value = ""
		DPrintf("%v timed out op %v. Client must retry\n", kv.me, op.toString())
	}

	// Clear entry for this channel
	kv.mu.Lock()
	delete(kv.appliedChs, opId)
	kv.mu.Unlock()

	return
}

//
// Executes a Get operation. Replies to the client with success and the
// value of the Get once the operation has been applied, or with failure/err
// if the client should retry on a different server.
//
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Do op
	success, err, value := kv.doOp(args.Key, "", Get, args.OpId)

	// Reply to client
	reply.Success = success
	reply.Err = err
	reply.Value = value
}

//
// Executes a Put/Append operation. Analogous to Get, but without returning
// a value upon success.
//
func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Do op
	success, err, _ := kv.doOp(args.Key, args.Value, args.Op, args.OpId)

	// Reply to client
	reply.Success = success
	reply.Err = err
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
	for {
		// Read next applied op
		applyMsg := <-kv.applyCh
		op := applyMsg.Command.(Op)

		DPrintf("%v got consensus for op %v\n. Applying...", kv.me, op.toString())

		// Apply op while locked
		go func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()

			// Apply op
			var value string
			switch op.Type {
			case Get:
				value = kv.kvMap[op.Key]
			case Put:
				kv.kvMap[op.Key] = op.Value
			case Append:
				kv.kvMap[op.Key] += op.Value
			}

			// Send message to the corresponding pending doOp(), if there
			// is one on this server for this op
			ch, ok := kv.appliedChs[op.Id]
			if ok {
				ch <- value
			}

			DPrintf("%v applied op %v\n", kv.me, op.toString())
		}()
	}
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
	DPrintf("%v shut down\n", kv.me)
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
	go kv.applyOps()

	DPrintf("%v booted up\n", me)

	return kv
}
