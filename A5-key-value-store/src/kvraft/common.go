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
	ClientId   int64
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

	// List of ops that this client has received "success" reply for
	CompletedOps []OpId
}

type PutAppendReply struct {
	//WrongLeader bool // NOTE: "Renamed" to describe more general outcome
	Success bool
	Err     Err
}


type GetArgs struct {
	Key  string
	OpId OpId

	// Piggy-backed data about other ops

	// List of ops that this client has received "success" reply for
	CompletedOps []OpId
}

type GetReply struct {
	//WrongLeader bool // NOTE: "Renamed" to describe more general outcome
	Success bool
	Err     Err
	Value   string
}
