package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"

	"dbg"
	"labrpc"
)

// import "bytes"
// import "encoding/gob"

//------------------------------------------------------------------------------

// Constants

// Timeout intervals, in milliseconds
const (
	ApplyInterval          = 70
	LeaderPeriodicInterval = 70
	RandomMaxInterval      = 300
	RandomMinInterval      = 150
	RPCTimeoutInterval     = 50
)

// State is a state of the Raft.
type State string

const (
	Follower  State = "Follower"
	Candidate State = "Candidate"
	Leader    State = "Leader"
)

// Log tags
const (
	// Functions
	tagAppendEntries   = "AppendEntries"
	tagApplyLogEntries = "applyLogEntries"
	tagBeCandidate     = "beCandidate"
	tagBeFollower      = "beFollower"
	tagGetState        = "GetState"
	tagBeLeader        = "beLeader"
	tagMake            = "Make"
	tagRequestVote     = "RequestVote"
	tagStart           = "Start"

	// Categories
	tagCandidate  = "candidate"
	tagConsensus  = "consensus"
	tagElection   = "election"
	tagFollower   = "follower"
	tagInactivity = "inactivity"
	tagLeader     = "leader"
	tagLock       = "lock"
	tagNewState   = "newState"
	tagSignal     = "signal"
)

//------------------------------------------------------------------------------

// Types

// AppendEntriesArgs contains the arguments for AppendEntries.
type AppendEntriesArgs struct {
	Entries      []LogEntry
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
	Term         int
}

// AppendEntriesReply contains the reply for AppendEntries.
type AppendEntriesReply struct {
	Success bool
	Term    int
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// LogEntry represents a log entry.
type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft implements a Raft server.
type Raft struct {
	CommitIndex          int
	ConvertToFollowerSig chan ConvertToFollowerSignal
	CurrentTerm          int
	Log                  []LogEntry
	Mu                   sync.Mutex
	Me                   int // index into peers[]
	Peers                []*labrpc.ClientEnd
	Persister            *Persister
	State                State // Follower, Candidate, or Leader
	VotedFor             int
}

// RequestVoteArgs contains the arguments for RequestVote.
type RequestVoteArgs struct {
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
	Term         int
}

// RequestVoteReply contains the reply for RequestVote.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// ConvertToFollowerSignal represents a Convert To Follower signal.
type ConvertToFollowerSignal struct {
	currentTerm int
	newTerm     int
	isLockHeld  bool // indicates whether the sender of the signal is holding a lock
	ackCh       chan bool
}

//------------------------------------------------------------------------------

// RPCs

// AppendEntries is the handler for receiving a AppendEntries RPC.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	dbg.LogKVs("Grabbing lock", []string{tagAppendEntries, tagLock}, map[string]interface{}{"rf": rf})
	rf.Mu.Lock()
	defer func() {
		dbg.LogKVs("Returning lock", []string{tagAppendEntries, tagLock}, map[string]interface{}{"rf": rf})
		rf.Mu.Unlock()
	}()

	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		dbg.LogKVs("Rejecting AppendEntries because of low term", []string{tagAppendEntries}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
		return
	}

	dbg.LogKVs("Sending Convert To Follower signal", []string{tagAppendEntries, tagElection, tagSignal}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
	rf.sendConvertToFollowerSig(rf.CurrentTerm, args.Term, true)

	if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		dbg.LogKVs("Rejecting AppendEntries because previous log entries don't match", []string{tagAppendEntries}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
		return
	}

	// Check for conflicting log entries
	myStartIndex := args.PrevLogIndex + 1
	for i := 0; i < min(len(args.Entries), len(rf.Log[myStartIndex:])); i++ {
		myIndex := myStartIndex + i
		if rf.Log[myIndex].Term != args.Entries[i].Term {
			dbg.LogKVs("Deleting conflicting entries from log", []string{tagAppendEntries, tagConsensus, tagFollower}, map[string]interface{}{"args": args, "i": i, "myIndex": myIndex, "myStartIndex": myStartIndex, "reply": reply, "rf": rf})
			rf.Log = rf.Log[:myIndex]
			break
		}
	}

	// Append new entries not already in the log
	entriesStartIndex := len(rf.Log) - myStartIndex
	dbg.LogKVsIf(entriesStartIndex < len(args.Entries), "Appending new entries to end of log", "", []string{tagAppendEntries, tagConsensus, tagFollower}, map[string]interface{}{"args": args, "entriesStartIndex": entriesStartIndex, "myStartIndex": myStartIndex, "reply": reply, "rf": rf})
	for i := entriesStartIndex; i < len(args.Entries); i++ {
		rf.Log = append(rf.Log, args.Entries[i])
	}

	// Update commit index
	if args.LeaderCommit > rf.CommitIndex {
		newCommitIndex := min(args.LeaderCommit, len(rf.Log)-1)
		dbg.LogKVs("Updating commit index", []string{tagAppendEntries, tagConsensus, tagFollower}, map[string]interface{}{"args": args, "newCommitIndex": newCommitIndex, "reply": reply, "rf": rf})
		rf.CommitIndex = newCommitIndex
	}
}

// RequestVote is the handler for receiving a RequestVote RPC.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	dbg.LogKVs("Grabbing lock", []string{tagElection, tagLock, tagRequestVote}, map[string]interface{}{"rf": rf})
	rf.Mu.Lock()
	defer func() {
		dbg.LogKVs("Returning lock", []string{tagElection, tagLock, tagRequestVote}, map[string]interface{}{"rf": rf})
		rf.Mu.Unlock()
	}()

	// Always send back my term, and default to not voting
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	if args.Term < rf.CurrentTerm {
		dbg.LogKVs("Rejecting invalid RequestVote", []string{tagElection, tagRequestVote}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
		return
	}

	if args.Term > rf.CurrentTerm {
		dbg.LogKVs("Sending Convert To Follower signal", []string{tagElection, tagRequestVote, tagSignal}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
		rf.sendConvertToFollowerSig(rf.CurrentTerm, args.Term, true)
	}

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
		// Check that my log is not more up to date than theirs
		myLastLogIndex := len(rf.Log) - 1
		myLastLogTerm := rf.Log[myLastLogIndex].Term
		if myLastLogTerm > args.LastLogTerm {
			return
		}
		if myLastLogTerm == args.LastLogTerm && myLastLogIndex > args.LastLogIndex {
			return
		}

		reply.VoteGranted = true
		rf.VotedFor = args.CandidateID
		dbg.LogKVs("Sending Convert To Follower signal", []string{tagElection, tagRequestVote, tagSignal}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
		rf.sendConvertToFollowerSig(rf.CurrentTerm, args.Term, true)
	}

	dbg.LogKVsIf(reply.VoteGranted, "Granting vote", "Denying vote", []string{tagElection, tagRequestVote}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
}

//------------------------------------------------------------------------------

// Exported functions

// GetState returns currentTerm and whether this server believes it is the
// leader.
func (rf *Raft) GetState() (int, bool) {
	dbg.LogKVs("Grabbing lock", []string{tagGetState, tagLock}, map[string]interface{}{"rf": rf})
	rf.Mu.Lock()
	defer func() {
		dbg.LogKVs("Returning lock", []string{tagGetState, tagLock}, map[string]interface{}{"rf": rf})
		rf.Mu.Unlock()
	}()

	currentTerm := rf.CurrentTerm
	isLeader := (rf.State == Leader)
	dbg.LogKVs("Returning state", []string{tagGetState}, map[string]interface{}{"rf": rf})
	return currentTerm, isLeader
}

// the tester calls Kill when a Raft instance won't be needed again.
func (rf *Raft) Kill() {

}

// Make creates a Raft server. The ports of all the Raft servers (including
// this one) are in peers[]. This server's port is peers[me]. All the servers'
// peers[] arrays have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most recent saved
// state, if any. applyCh is a channel on which the tester or service expects
// Raft to send ApplyMsg messages. Make must return quickly, so it should
// start goroutines for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// Initialize
	rf := &Raft{
		CurrentTerm: -1,
		Log:         make([]LogEntry, 1), // initialize with dummy value at index 0
		Me:          me,
		Peers:       peers,
		Persister:   persister,
		State:       "",
		VotedFor:    -1,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Enter Follower state
	dbg.LogKVs("Initialized Raft server", []string{tagMake}, map[string]interface{}{"rf": rf})
	go rf.beFollower(0, false, nil)
	go rf.applyLogEntries(applyCh)

	return rf
}

// Start submits a command for the Raft servers to execute. If this server
// isn't the leader, returns false. Otherwise, submits the command and returns
// immediately.
//
// Note that there is no guarantee that this command will ever be committed to
// the log, since the leader may fail or lose an election.
//
// The first return value is the index that the command will appear at if it's
// ever committed. the second is the current term, and the third is true if
// this server believes it is the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	dbg.LogKVs("Grabbing lock", []string{tagLock, tagStart}, map[string]interface{}{"rf": rf})
	rf.Mu.Lock()
	defer func() {
		dbg.LogKVs("Returning lock", []string{tagLock, tagStart}, map[string]interface{}{"rf": rf})
		rf.Mu.Unlock()
	}()

	if rf.State == Leader {
		// Append entry to log
		entry := LogEntry{rf.CurrentTerm, command}
		dbg.LogKVs("Appending new entry to log", []string{tagConsensus, tagLeader, tagStart}, map[string]interface{}{"entry": entry, "rf": rf})
		rf.Log = append(rf.Log, entry)
		index := len(rf.Log) - 1
		return index, rf.CurrentTerm, true
	}
	dbg.LogKVs("Rejecting command because not leader", []string{tagConsensus, tagStart}, map[string]interface{}{"rf": rf})
	return -1, rf.CurrentTerm, false
}

//------------------------------------------------------------------------------

// Private functions

// applyLogEntries periodically checks for any committed entries that have not
// yet been applied, and applies them. It runs indefinitely.
func (rf *Raft) applyLogEntries(applyCh chan ApplyMsg) {
	lastApplied := 0
	for {
		dbg.LogKVs("Grabbing lock", []string{tagLock, tagApplyLogEntries}, map[string]interface{}{"lastApplied": lastApplied, "rf": rf})
		rf.Mu.Lock()
		if rf.CommitIndex > lastApplied {
			lastApplied++
			applyMsg := ApplyMsg{
				Index:   lastApplied,
				Command: rf.Log[lastApplied].Command,
			}
			dbg.LogKVs("Applying log entry", []string{tagApplyLogEntries}, map[string]interface{}{"applyMsg": applyMsg, "rf": rf})
			applyCh <- applyMsg
		}
		dbg.LogKVs("Returning lock", []string{tagLock, tagApplyLogEntries}, map[string]interface{}{"lastApplied": lastApplied, "rf": rf})
		rf.Mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(ApplyInterval))
	}
}

// beCandidate runs the Candidate state for a particular term.
func (rf *Raft) beCandidate() {
	// Set up Candidate
	dbg.LogKVs("Grabbing lock", []string{tagBeCandidate, tagCandidate, tagElection, tagLock}, map[string]interface{}{"rf": rf})
	rf.Mu.Lock()
	me := rf.Me
	npeers := len(rf.Peers)
	rf.State = Candidate
	rf.CurrentTerm++
	currentTerm := rf.CurrentTerm
	rf.VotedFor = rf.Me
	numVotes := 1

	dbg.LogKVs("Entered Candidate state", []string{tagBeCandidate, tagCandidate, tagElection, tagNewState}, map[string]interface{}{"rf": rf})

	// Send RequestVote RPCs
	lastLogIndex := len(rf.Log) - 1
	args := RequestVoteArgs{me, lastLogIndex, rf.Log[lastLogIndex].Term, currentTerm}
	votesCh := make(chan bool, npeers)
	for i := 0; i < npeers; i++ {
		if i == me {
			continue
		}

		go func(i int, args RequestVoteArgs) {
			dbg.LogKVs("Sending RequestVote", []string{tagBeCandidate, tagCandidate, tagElection}, map[string]interface{}{"i": i, "args": args, "rf": rf})
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, &reply)
			if !ok {
				return // ignore failed RPCs
			}

			if reply.Term > currentTerm {
				dbg.LogKVs("Sending Convert To Follower signal", []string{tagBeCandidate, tagCandidate, tagElection, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "reply": reply, "rf": rf})
				rf.sendConvertToFollowerSig(currentTerm, reply.Term, false)
				return
			}

			if reply.VoteGranted {
				dbg.LogKVs("Got RequestVote reply", []string{tagBeCandidate, tagCandidate, tagElection}, map[string]interface{}{"i": i, "reply": reply, "rf": rf})
				votesCh <- true
			}
		}(i, args)
	}

	dbg.LogKVs("Returning lock", []string{tagBeCandidate, tagCandidate, tagElection, tagLock}, map[string]interface{}{"rf": rf})
	rf.Mu.Unlock()

	// Tally votes in the background
	electionTimer := makeRandomTimer()
	winSig := make(chan bool, 1)
	go func() {
		for i := 0; i < npeers; i++ {
			<-votesCh
			numVotes++
			if numVotes == (npeers/2)+1 {
				dbg.LogKVs("Sending Win signal", []string{tagBeCandidate, tagCandidate, tagElection, tagSignal}, map[string]interface{}{"numVotes": numVotes, "rf": rf})
				winSig <- true
				break
			}
		}
	}()

	// Wait for signals to decide next state
waitForValidSignal:
	for {
		select {
		case <-winSig:
			dbg.LogKVs("Received Win signal", []string{tagBeCandidate, tagCandidate, tagElection, tagSignal}, map[string]interface{}{"rf": rf})
			go rf.beLeader(currentTerm, nil, nil)
			break waitForValidSignal
		case sig := <-rf.ConvertToFollowerSig:
			if sig.currentTerm != currentTerm {
				dbg.LogKVs("Received Convert To Follower signal, ignoring because term is wrong", []string{tagBeCandidate, tagCandidate, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "sig": sig})
				continue
			}

			dbg.LogKVs("Received Convert To Follower signal, valid", []string{tagBeCandidate, tagCandidate, tagElection, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "sig": sig})
			go rf.beFollower(sig.newTerm, sig.isLockHeld, sig.ackCh)
			break waitForValidSignal
		case <-electionTimer.C:
			dbg.LogKVs("Election timer timed out", []string{tagBeCandidate, tagCandidate, tagElection, tagSignal}, map[string]interface{}{"rf": rf})
			go rf.beCandidate()
			break waitForValidSignal
		}
	}
}

// beFollower runs the Follower state for a particular term. Pass newTerm as
// arg, which is either a later term if one is known at the time of calling, or
// the current term if not. isLockHeld indicates whether the caller is currently
// holding a lock. Send true on the ackCh when the conversion to the Follower
// state is complete.
func (rf *Raft) beFollower(newTerm int, isLockHeld bool, ackCh chan bool) {
	// Set up Follower
	if !isLockHeld {
		dbg.LogKVs("Grabbing lock", []string{tagBeFollower, tagFollower, tagLock}, map[string]interface{}{"rf": rf})
		rf.Mu.Lock()
	}

	rf.State = Follower
	updatedTerm := false
	if newTerm > rf.CurrentTerm {
		rf.CurrentTerm = newTerm
		rf.VotedFor = -1
		updatedTerm = true
	}
	currentTerm := rf.CurrentTerm

	if !isLockHeld {
		dbg.LogKVs("Returning lock", []string{tagBeFollower, tagFollower, tagLock}, map[string]interface{}{"rf": rf})
		rf.Mu.Unlock()
	}

	dbg.LogKVsIf(updatedTerm, "Entered Follower state in a new term", "", []string{tagBeFollower, tagFollower, tagNewState}, map[string]interface{}{"newTerm": newTerm, "rf": rf})
	dbg.LogKVsIf(!updatedTerm, "Re-entered Follower state in same term", "", []string{tagBeFollower, tagFollower, tagInactivity}, map[string]interface{}{"newTerm": newTerm, "rf": rf})
	ackCh <- true

	electionTimer := makeRandomTimer()

	// Wait for signals to decide next state
waitForValidSignal:
	for {
		select {
		case <-electionTimer.C:
			dbg.LogKVs("Election timer timed out", []string{tagBeFollower, tagFollower, tagSignal}, map[string]interface{}{"rf": rf})
			go rf.beCandidate()
			break waitForValidSignal
		case sig := <-rf.ConvertToFollowerSig:
			if sig.currentTerm != currentTerm {
				dbg.LogKVs("Received Convert To Follower signal, ignoring because term is wrong", []string{tagBeFollower, tagFollower, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "sig": sig})
				continue
			}

			dbg.LogKVs("Received Convert To Follower signal, valid", []string{tagBeFollower, tagFollower, tagInactivity, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "sig": sig})
			go rf.beFollower(sig.newTerm, sig.isLockHeld, sig.ackCh)
			break waitForValidSignal
		}
	}
}

// beLeader runs the Leader state for a particular term. currentTerm is the term
// for which this server is leader. nextIndex and matchIndex are optional args -
// if this leader was just newly elected, pass nil for these values. Otherwise,
// pass the nextIndex and matchIndex from the last periodic interval.
func (rf *Raft) beLeader(currentTerm int, nextIndex []int, matchIndex []int) {
	dbg.LogKVs("Grabbing lock", []string{tagBeLeader, tagLeader, tagLock}, map[string]interface{}{"rf": rf})
	rf.Mu.Lock()

	// Check if received a signal since trying to grab the lock, otherwise proceed
checkForValidSignal:
	for {
		select {
		case sig := <-rf.ConvertToFollowerSig:
			if sig.currentTerm != currentTerm {
				dbg.LogKVs("Received Convert To Follower signal, ignoring because term is wrong", []string{tagBeLeader, tagLeader, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "sig": sig})
				continue
			}

			dbg.LogKVs("Received Convert To Follower signal, valid", []string{tagBeLeader, tagLeader, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "sig": sig})
			rf.Mu.Unlock()
			go rf.beFollower(sig.newTerm, sig.isLockHeld, sig.ackCh)
			return
		default:
			break checkForValidSignal
		}
	}

	// Set up Leader
	me := rf.Me
	rf.State = Leader

	newlyElected := (nextIndex == nil)
	if newlyElected {
		// Initialize nextIndex and matchIndex
		nextIndex = make([]int, len(rf.Peers))
		matchIndex = make([]int, len(rf.Peers))
		for i := range rf.Peers {
			if i == me {
				continue
			}

			nextIndex[i] = len(rf.Log)
			matchIndex[i] = 0
		}
	}

	dbg.LogKVsIf(newlyElected, "Entered Leader state, newly elected", "", []string{tagBeLeader, tagLeader, tagNewState}, map[string]interface{}{"currentTerm": currentTerm, "matchIndex": matchIndex, "nextIndex": nextIndex, "rf": rf})
	dbg.LogKVsIf(!newlyElected, "Re-entered Leader state after periodic interval", "", []string{tagBeLeader, tagInactivity, tagLeader}, map[string]interface{}{"currentTerm": currentTerm, "matchIndex": matchIndex, "nextIndex": nextIndex, "rf": rf})

	// Send a round of AppendEntries
	for i := range rf.Peers {
		if i == me {
			continue
		}

		// Construct args
		entries := make([]LogEntry, 0)
		for j := nextIndex[i]; j < len(rf.Log); j++ {
			entries = append(entries, rf.Log[j])
		}
		prevLogIndex := nextIndex[i] - 1
		prevLogTerm := rf.Log[prevLogIndex].Term
		args := AppendEntriesArgs{entries, rf.CommitIndex, prevLogIndex, prevLogTerm, currentTerm}

		// Send
		go func(i int, args AppendEntriesArgs) {
			dbg.LogKVsIf(len(entries) == 0, "Sending AppendEntries without new entries", "", []string{tagBeLeader, tagInactivity, tagLeader}, map[string]interface{}{"i": i, "args": args, "rf": rf})
			dbg.LogKVsIf(len(entries) != 0, "Sending AppendEntries with new entries", "", []string{tagBeLeader, tagConsensus, tagLeader}, map[string]interface{}{"i": i, "args": args, "rf": rf})
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, args, &reply)
			if !ok {
				return // ignore, try again next interval
			}

			if reply.Term > currentTerm {
				dbg.LogKVs("Sending Convert To Follower signal", []string{tagBeLeader, tagConsensus, tagLeader, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "reply.Term": reply.Term, "rf": rf})
				rf.sendConvertToFollowerSig(currentTerm, reply.Term, false)
				return
			}

			if !reply.Success {
				nextIndex[i]--
				return
			}

			matchIndex[i] = args.PrevLogIndex + len(args.Entries)
			nextIndex[i] = matchIndex[i] + 1
		}(i, args)
	}

	// Check to update the commit index
	for i := len(rf.Log) - 1; i > rf.CommitIndex; i-- {
		if rf.Log[i].Term != currentTerm {
			continue
		}

		// Count number of servers with matchIndex >= this log index
		count := 1 // 1 for me
		for j := range matchIndex {
			if j == me {
				continue
			}

			if matchIndex[j] >= i {
				count++
			}
		}
		if count >= (len(rf.Peers)/2)+1 {
			dbg.LogKVs("Updating commit index", []string{tagBeLeader, tagConsensus, tagLeader}, map[string]interface{}{"count": count, "currentTerm": currentTerm, "i": i, "matchIndex": matchIndex, "nextIndex": nextIndex, "rf": rf})
			rf.CommitIndex = i
			break
		}
	}

	dbg.LogKVs("Returning lock", []string{tagBeLeader, tagLeader, tagLock}, map[string]interface{}{"rf": rf})
	rf.Mu.Unlock()

	periodicTimer := time.NewTimer(time.Millisecond * time.Duration(LeaderPeriodicInterval))

	// Wait for signals to determine next state
waitForValidSignal:
	for {
		select {
		case sig := <-rf.ConvertToFollowerSig:
			if sig.currentTerm != currentTerm {
				dbg.LogKVs("Received Convert To Follower signal, ignoring because term is wrong", []string{tagBeLeader, tagLeader, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "sig": sig})
				continue
			}

			dbg.LogKVs("Received Convert To Follower signal, valid", []string{tagBeLeader, tagLeader, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "sig": sig})
			go rf.beFollower(sig.newTerm, sig.isLockHeld, sig.ackCh)
			break waitForValidSignal
		case <-periodicTimer.C:
			dbg.LogKVs("Leader periodic interval timed out", []string{tagBeLeader, tagInactivity, tagLeader, tagSignal}, map[string]interface{}{"rf": rf})
			go rf.beLeader(currentTerm, nextIndex, matchIndex)
			break waitForValidSignal
		}
	}
}

// min returns the min of two numbers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

// makeRandomTimer returns a timer of random duration.
func makeRandomTimer() *time.Timer {
	r := RandomMinInterval + rand.Intn(RandomMaxInterval-RandomMinInterval)
	return time.NewTimer(time.Millisecond * time.Duration(r))
}

// sendAppendEntries is a wrapper for sending an AppendEntries RPC to `server`.
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	okCh := make(chan bool, 1)
	go func() {
		okCh <- rf.Peers[server].Call("Raft.AppendEntries", args, reply)
	}()

	timeout := time.NewTimer(time.Millisecond * time.Duration(RPCTimeoutInterval))
	select {
	case ok := <-okCh:
		return ok
	case <-timeout.C:
		return false
	}
}

// sendConvertToFollowerSig is a wrapper for sending a Convert To Follower signal and
// waiting for an ack.
func (rf *Raft) sendConvertToFollowerSig(currentTerm int, newTerm int, isLockHeld bool) {
	ackCh := make(chan bool, 1)
	rf.ConvertToFollowerSig <- ConvertToFollowerSignal{currentTerm, newTerm, isLockHeld, ackCh}
	<-ackCh
}

// sendRequestVote is a wrapper for sending a RequestVote RPC to `server`.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	okCh := make(chan bool, 1)
	go func() {
		okCh <- rf.Peers[server].Call("Raft.RequestVote", args, reply)
	}()

	timeout := time.NewTimer(time.Millisecond * time.Duration(RPCTimeoutInterval))
	select {
	case ok := <-okCh:
		return ok
	case <-timeout.C:
		return false
	}
}
