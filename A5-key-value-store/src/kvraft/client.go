package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

//-----------------------------------------------------------------------------

// Clerk definition/initialization

type Clerk struct {
	servers []*labrpc.ClientEnd

	id            int64  // my client id
	currentOpId   int    // value to be used as OpId.ClientOpId
	completedOps  []OpId
	currentLeader int    // index into servers
}

//
// Returns a random 64-bit integer
//
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//
// Returns a new Clerk, given the array of servers it
// can communicate with to execute operations.
//
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	// Generate a random ID for this clerk
	ck.id = nrand()

	// Initialize other clerk fields
	ck.currentOpId = 0
	ck.completedOps = make([]OpId, 0)
	ck.currentLeader = 0

	return ck
}

//-----------------------------------------------------------------------------

// Ops

//
// Makes a request to the servers to execute the op of type t (Get, Put, or
// Append), given the key and, for Put/Append, the value (use the empty string
// as the value for a Get). For a Get, returns the value, or empty string if
// the key doesn't exist. For Put/Append, returns the empty string for.
//
// You can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// Remember that the types of args and reply (including whether they are
// pointers) must match the declared types of the RPC handler function's
// arguments. And reply must be passed as a pointer.
//
func (ck *Clerk) handleOp(key string, value string, t string) string {
	// Construct next op ID
	opId := OpId{ck.id, ck.currentOpId}
	ck.currentOpId++

	// Keep retrying op until we get a successful response
	var replyValue string
	for {
		// Make a request for this op to the current leader
		var ok bool
		var success bool
		if t == Get {
			args := GetArgs{
				key,
				opId,
				ck.completedOps,
			}

			reply := GetReply{}

			ok = ck.servers[ck.currentLeader].Call("RaftKV.Get", &args, &reply)
			success = reply.Success
			replyValue = reply.Value

		} else if t == Put || t == Append {
			args := PutAppendArgs{
				key,
				value,
				t,
				opId,
				ck.completedOps,
			}

			reply := PutAppendReply{}

			ok = ck.servers[ck.currentLeader].Call("RaftKV.PutAppend", &args, &reply)
			success = reply.Success
			replyValue = ""
		}

		// Check if reply was successful
		if ok && success {
			// Op is done!
			break
		} else {
			// Op was unsuccessful. Retry on the next server
			ck.currentLeader++
		}
	}

	// Add op to completed ops
	ck.completedOps = append(ck.completedOps, opId)

	return replyValue
}


//
// Fetches and returns the current value for a key, or returns "" if the
// key does not exist.
//
func (ck *Clerk) Get(key string) string {
	return ck.handleOp(key, "", Get)
}


//
// Inserts a key-value pair.
//
func (ck *Clerk) Put(key string, value string) {
	ck.handleOp(key, value, Put)
}

//
// Appends `value` to the current value for `key`.
//
func (ck *Clerk) Append(key string, value string) {
	ck.handleOp(key, value, Append)
}
