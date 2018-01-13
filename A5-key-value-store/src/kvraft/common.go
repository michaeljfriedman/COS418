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
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	//WrongLeader bool // NOTE: "Renamed" to describe more general outcome
	Success bool
	Err     Err
}

type GetArgs struct {
	Key  string
	OpId OpId
	// You'll have to add definitions here.
}

type GetReply struct {
	//WrongLeader bool // NOTE: "Renamed" to describe more general outcome
	Success bool
	Err     Err
	Value   string
}
