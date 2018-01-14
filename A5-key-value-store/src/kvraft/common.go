package raftkv

// Constants for errors
const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Constants for Op types
const (
	Get    = "Get"
	Put    = "Put"
	Append = "Append"
)

// Unique IDs for Ops. 2-tuple (Client ID, Client Op ID)
type OpId struct {
	ClientId   int
	ClientOpId int
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	OpId  OpId

	// Piggy-backed data about other ops

	// List of ops for which this client has received "success" replies
	AckedOps []OpId
}

type PutAppendReply struct {
	//WrongLeader bool // NOTE: "Renamed" to describe more general outcome
	Success bool
	Err     Err

	// Piggy-backed data about other ops

	// All other ops from this client that the server applied, but has not yet
	// replied to this client. Key is op id, value is always empty string.
	AppliedOps map[OpId]string
}


type GetArgs struct {
	Key  string
	OpId OpId

	// Piggy-backed data about other ops

	// List of ops for which this client has received "success" replies
	AckedOps []OpId
}

type GetReply struct {
	//WrongLeader bool // NOTE: "Renamed" to describe more general outcome
	Success bool
	Err     Err
	Value   string

	// Piggy-backed data about other ops

	// All other ops from this client that the server applied, but has not yet
	// replied to this client. Key is op id, value is the result of Get.
	AppliedOps map[OpId]string
}
