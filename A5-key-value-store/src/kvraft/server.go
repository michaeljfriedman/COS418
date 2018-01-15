package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"raft"
	"sync"
	"time"
)

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

	// ... However, if an op is applied *after* its corresponding handleOp() times
	// out (i.e. after a "failure" reply has already gone back to the client),
	// the op is instead placed on this back log, and the server sends "success"
	// for all back-logged ops in its next reply to the client.
	appliedBackLog map[OpId]string

	// Indicates whether an op has been started in Raft. (Just the existence of
	// an entry in this map indicates it has been started. If it has not, or if
	// it has been acknowledged as completed by the client, there is no entry.)
	isStarted map[OpId]bool
}

//------------------------------------------------------------------------------

// Get/Put/Append ops

// Timeout for an operation to commit/be applied, in milliseconds.
const OpTimeout = 1000


//
// Clears up state maintained about ops that the client has acknolwedged are
// completed: back log and isStarted map.
//
func (kv *RaftKV) freeMemForCompletedOps(completedOps []OpId) {
	DPrintf(LockStream, "Server %v is grabbing lock 3\n", kv.me)
	kv.mu.Lock()
	defer func() {
		DPrintf(LockStream, "Server %v is releasing lock 3\n", kv.me)
		kv.mu.Unlock()
	}()

	for _, opId := range completedOps {
		delete(kv.appliedBackLog, opId)
		delete(kv.isStarted, opId)
	}
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
func (kv *RaftKV) handleOp(key string, putValue string, t string, opId OpId, completedOps []OpId) (success bool, err Err, value string) {
	op := Op{key, putValue, t, opId}
	DPrintf(DefaultStream, "Server %v got op %v\n", kv.me, op.toString())

	// Clear state about "stale" ops (i.e. those the client acknowledges
	// are completed)
	kv.freeMemForCompletedOps(completedOps)
	DPrintf(DefaultStream, "Server %v cleared state about completed ops: %v\n", kv.me, completedOps)

	//---------------

	// Do the op

	// First, check some conditions before starting a new op in Raft. Must be
	// locked for safety. `proceed` indicates whether or not to proceed after
	// this locked section finishes.
	DPrintf(LockStream, "Server %v is grabbing lock 5\n", kv.me)
	proceed := func() bool {
		kv.mu.Lock()
		defer func() {
			DPrintf(LockStream, "Server %v is releasing lock 5\n", kv.me)
			kv.mu.Unlock()
		}()

		// First, check if result of op is in the back log
		backLogValue, ok := kv.appliedBackLog[opId]
		if ok {
			// Result is in back log. Return it
			success = true
			err = OK
			value = backLogValue

			DPrintf(DefaultStream, "Server %v got op %v from back log. Returning result\n", kv.me, op.toString())
			return false
		}

		// Check if op has already been started
		_, ok = kv.isStarted[opId]
		if ok {
			// Op has already been started. Tell client to retry for result in a bit.
			success = false
			err = ErrStartedAlready
			value = ""

			DPrintf(DefaultStream, "Server %v already started op %v. Not proceeding\n", kv.me, op.toString())
			return false
		}

		return true
	}()

	if !proceed {
		return
	}

	// Op not started yet. Try to start this op in Raft, making sure that I am
	// the leader. Do not proceed if I'm not.
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		success = false
		err = ErrWrongLeader
		value = ""

		DPrintf(DefaultStream, "Server %v is not leader for op %v. Not proceeding\n", kv.me, op.toString())
		return
	}

	// Set up a channel with applyOps() to receive result of the op. Return value
	// of this locked section is that channel.
	appliedCh := func() chan string  {
		DPrintf(LockStream, "Server %v is grabbing lock 1\n", kv.me)
		kv.mu.Lock()
		defer func() {
			DPrintf(LockStream, "Server %v is releasing lock 1\n", kv.me)
			kv.mu.Unlock()
		}()

		// Mark op started
		// DPrintf(DefaultStream, "Server %v marked 'started' op %v\n", kv.me, op.toString())
		kv.isStarted[opId] = true

		// Set up channel
		ch := make(chan string, 1)
		kv.appliedChs[opId] = ch
		return ch
	}()

	// Wait for result from applyOps(), or time out
	timer := time.NewTimer(time.Millisecond * time.Duration(OpTimeout))
	DPrintf(DefaultStream, "Server %v started op %v. Waiting for completion...\n", kv.me, op.toString())
	select {
	case value = <-appliedCh:
		// Op was successfully applied
		success = true
		err = OK
		DPrintf(DefaultStream, "Server %v is done with op %v\n", kv.me, op.toString())
	case <-timer.C:
		// Timed out
		success = false
		err = ErrTimeout
		value = ""
		DPrintf(DefaultStream, "Server %v timed out op %v. Client must retry\n", kv.me, op.toString())
	}

	// Clear entry for this channel
	DPrintf(LockStream, "Server %v is grabbing lock 2\n", kv.me)
	kv.mu.Lock()
	delete(kv.appliedChs, opId)
	DPrintf(LockStream, "Server %v is grabbing lock 2\n", kv.me)
	kv.mu.Unlock()

	return
}


//
// Executes a Get operation. Replies to the client with success and the
// value of the Get once the operation has been applied, or with failure/err
// if the client should retry on a different server.
//
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Handle op
	success, err, value := kv.handleOp(args.Key, "", Get, args.OpId, args.CompletedOps)

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
	// Handle op
	success, err, _ := kv.handleOp(args.Key, args.Value, args.Op, args.OpId, args.CompletedOps)

	// Reply to client
	reply.Success = success
	reply.Err = err
}

//------------------------------------------------------------------------------

// Applying operations

//
// Continuously reads from the applyCh for new ops to apply. When an op comes
// in, applies it, and sends the op's result to the pending handleOp() via the
// corresponding chan in appliedChs, if there is one. If there isn't, adds
// the op to the back log.
//
// The result will contain either the value, if it's a Get op, or the empty
// string, if it's a Put/Append.
//
func (kv *RaftKV) applyOps() {
	for {
		// Read next applied op
		applyMsg := <-kv.applyCh
		op := applyMsg.Command.(Op)

		DPrintf(DefaultStream, "Server %v got consensus for op %v. Applying...\n", kv.me, op.toString())

		// Apply op while locked
		func() {
			DPrintf(LockStream, "Server %v is grabbing lock 4\n", kv.me)
			kv.mu.Lock()
			defer func() {
				DPrintf(LockStream, "Server %v is releasing lock 4\n", kv.me)
				kv.mu.Unlock()
			}()

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

			DPrintf(DefaultStream, "Server %v applied op %v\n", kv.me, op.toString())

			// Send result to the corresponding pending handleOp(), if there
			// is one for this op
			ch, ok := kv.appliedChs[op.Id]
			if ok {
				ch <- value
				DPrintf(DefaultStream, "Server %v sent result of op %v back to handleOp()\n", kv.me, op.toString())
			} else {
				// No pending handleOp(). Add this op to back log instead
				kv.appliedBackLog[op.Id] = value
				DPrintf(DefaultStream, "Server %v added result of op %v to back log\n", kv.me, op.toString())
			}
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
	DPrintf(DefaultStream, "Server %v shut down\n", kv.me)
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
	kv.kvMap = make(map[string]string)
	kv.appliedChs = make(map[OpId](chan string))
	kv.appliedBackLog = make(map[OpId]string)
	kv.isStarted = make(map[OpId]bool)

	// Start waiting for applied ops
	go kv.applyOps()

	DPrintf(DefaultStream, "Server %v booted up\n", me)

	return kv
}
