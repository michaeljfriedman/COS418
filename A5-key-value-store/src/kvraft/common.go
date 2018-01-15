package raftkv

import (
	"fmt"
	"log"
)

//-----------------------------------------------------------------------------

// Debugging tools

const Debug = 1

// Debugging streams
const (
	DefaultStream = "Default"
	LockStream    = "Lock"
)

func DPrintf(stream string, format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		streamPrefix := fmt.Sprintf("[%v] ", stream)
		log.Printf(streamPrefix+format, a...)
	}
	return
}

//-----------------------------------------------------------------------------

// Constants for errors
const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrStartedAlready = "ErrStartedAlready"
	ErrTimeout        = "ErrTimeout"
	ErrWrongLeader    = "ErrWrongLeader"
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
