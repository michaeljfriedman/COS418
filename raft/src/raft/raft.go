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
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"

	"dbg"
	"labrpc"
)

//------------------------------------------------------------------------------

// Constants

// Timeout intervals, in milliseconds
const (
	ApplyInterval          = 75
	LeaderPeriodicInterval = 35
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
	tagKill            = "Kill"
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
	NextIndex int
	Success   bool
	Term      int
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

// FollowerParams is a container for Follower state's parameters.
type FollowerParams struct {
	NewTerm    int
	IsLockHeld bool
	ackCh      chan bool
}

// LeaderParams is a container fot the Leader state's parameters
type LeaderParams struct {
	currentTerm int
	nextIndex   []int
	matchIndex  []int
}

// LogEntry represents a log entry.
type LogEntry struct {
	Term    int
	Command interface{}
}

// Params is a container for params returned by state handlers.
type Params struct {
	follower FollowerParams
	leader   LeaderParams
}

// Raft implements a Raft server.
type Raft struct {
	CommitIndex          int
	convertToFollowerSig chan FollowerParams
	CurrentTerm          int
	killCh               chan bool
	Log                  []LogEntry
	mu                   sync.Mutex
	Me                   int // index into peers[]
	peers                []*labrpc.ClientEnd
	persister            *Persister
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

//------------------------------------------------------------------------------

// RPCs

// AppendEntries is the handler for receiving a AppendEntries RPC.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	dbg.LogKVs("Grabbing lock", []string{tagAppendEntries, tagLock}, map[string]interface{}{"rf": rf})
	rf.mu.Lock()
	defer func() {
		dbg.LogKVs("Returning lock", []string{tagAppendEntries, tagLock}, map[string]interface{}{"rf": rf})
		rf.mu.Unlock()
	}()

	dbg.LogKVsIf(len(args.Entries) == 0 && args.LeaderCommit <= rf.CommitIndex, "Received AppendEntries", "", []string{tagAppendEntries, tagInactivity}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
	dbg.LogKVsIf(len(args.Entries) > 0 || args.LeaderCommit > rf.CommitIndex, "Received AppendEntries", "", []string{tagAppendEntries, tagConsensus}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})

	// Always reply with my term, and default to failure
	reply.Term = rf.CurrentTerm
	reply.Success = false

	if args.Term < rf.CurrentTerm {
		dbg.LogKVs("Rejecting AppendEntries because of low term", []string{tagAppendEntries, tagElection}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
		return
	}

	dbg.LogKVsIf(len(args.Entries) == 0 && args.LeaderCommit <= rf.CommitIndex, "Sending Convert To Follower signal", "", []string{tagAppendEntries, tagElection, tagInactivity, tagSignal}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
	dbg.LogKVsIf(len(args.Entries) > 0 || args.LeaderCommit > rf.CommitIndex, "Sending Convert To Follower signal", "", []string{tagAppendEntries, tagConsensus, tagElection, tagSignal}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
	rf.sendConvertToFollowerSig(args.Term, true)

	if args.PrevLogIndex > len(rf.Log)-1 {
		reply.NextIndex = len(rf.Log)
		dbg.LogKVs("Rejecting AppendEntries because previous log index above upper bound of log", []string{tagAppendEntries}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
		return
	}

	if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictingTerm := rf.Log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.Log[i].Term != conflictingTerm {
				reply.NextIndex = i + 1
				break
			}
		}
		dbg.LogKVs("Rejecting AppendEntries because previous log entries don't match", []string{tagAppendEntries}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
		return
	}

	reply.Success = true

	// Check for conflicting log entries
	myStartIndex := args.PrevLogIndex + 1
	for i := 0; i < min(len(args.Entries), len(rf.Log[args.PrevLogIndex:])-1); i++ {
		myIndex := myStartIndex + i
		if rf.Log[myIndex].Term != args.Entries[i].Term {
			dbg.LogKVs("Deleting conflicting entries from log", []string{tagAppendEntries, tagConsensus, tagFollower}, map[string]interface{}{"args": args, "i": i, "myIndex": myIndex, "myStartIndex": myStartIndex, "reply": reply, "rf": rf})
			rf.Log = rf.Log[:myIndex]
			break
		}
	}

	// Append new entries not already in the log
	entriesStartIndex := len(rf.Log) - myStartIndex
	dbg.LogKVsIf(entriesStartIndex < len(args.Entries), "Appending new entries to log", "", []string{tagAppendEntries, tagConsensus, tagFollower}, map[string]interface{}{"args": args, "entriesStartIndex": entriesStartIndex, "myStartIndex": myStartIndex, "reply": reply, "rf": rf})
	for i := entriesStartIndex; i < len(args.Entries); i++ {
		rf.Log = append(rf.Log, args.Entries[i])
	}

	// Update commit index
	if args.LeaderCommit > rf.CommitIndex {
		newCommitIndex := min(args.LeaderCommit, len(rf.Log)-1)
		dbg.LogKVs("Updating commit index", []string{tagAppendEntries, tagConsensus, tagFollower}, map[string]interface{}{"args": args, "newCommitIndex": newCommitIndex, "reply": reply, "rf": rf})
		rf.CommitIndex = newCommitIndex
	}

	rf.persist(true)
}

// RequestVote is the handler for receiving a RequestVote RPC.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	dbg.LogKVs("Grabbing lock", []string{tagElection, tagLock, tagRequestVote}, map[string]interface{}{"rf": rf})
	rf.mu.Lock()
	defer func() {
		dbg.LogKVsIf(reply.VoteGranted, "Granting vote", "Denying vote", []string{tagElection, tagRequestVote}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
		dbg.LogKVs("Returning lock", []string{tagElection, tagLock, tagRequestVote}, map[string]interface{}{"rf": rf})
		rf.mu.Unlock()
	}()

	dbg.LogKVs("Received RequestVote", []string{tagElection, tagRequestVote}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})

	// Always send back my term, and default to not voting
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	if args.Term < rf.CurrentTerm {
		dbg.LogKVs("Rejecting invalid RequestVote", []string{tagElection, tagRequestVote}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
		return
	}

	if args.Term > rf.CurrentTerm {
		dbg.LogKVs("Sending Convert To Follower signal", []string{tagElection, tagRequestVote, tagSignal}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
		rf.sendConvertToFollowerSig(args.Term, true)
	}

	// Check that I haven't voted yet and that my log is not more up to date
	// then theirs
	if rf.VotedFor != -1 && rf.VotedFor != args.CandidateID {
		return
	}

	myLastLogIndex := len(rf.Log) - 1
	myLastLogTerm := rf.Log[myLastLogIndex].Term
	if myLastLogTerm > args.LastLogTerm {
		return
	}
	if myLastLogTerm == args.LastLogTerm && myLastLogIndex > args.LastLogIndex {
		return
	}

	// Grant vote
	reply.VoteGranted = true
	rf.VotedFor = args.CandidateID
	rf.persist(true)
	dbg.LogKVs("Sending Convert To Follower signal", []string{tagElection, tagRequestVote, tagSignal}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
	rf.sendConvertToFollowerSig(args.Term, true)
}

//------------------------------------------------------------------------------

// Exported functions

// GetState returns currentTerm and whether this server believes it is the
// leader.
func (rf *Raft) GetState() (int, bool) {
	dbg.LogKVs("Grabbing lock", []string{tagGetState, tagLock}, map[string]interface{}{"rf": rf})
	rf.mu.Lock()
	defer func() {
		dbg.LogKVs("Returning lock", []string{tagGetState, tagLock}, map[string]interface{}{"rf": rf})
		rf.mu.Unlock()
	}()

	currentTerm := rf.CurrentTerm
	isLeader := (rf.State == Leader)
	dbg.LogKVs("Returning state", []string{tagGetState}, map[string]interface{}{"rf": rf})
	return currentTerm, isLeader
}

// Kill should kill this server. The tester calls it when a Raft instance won't
// be needed again.
func (rf *Raft) Kill() {
	dbg.LogKVs("Killing Raft server", []string{tagKill}, map[string]interface{}{"rf": rf})
	rf.killCh <- true
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
		CommitIndex:          0,
		convertToFollowerSig: make(chan FollowerParams, len(peers)),
		CurrentTerm:          -1,
		killCh:               make(chan bool, 1),
		Log:                  make([]LogEntry, 1), // initialize with dummy value at index 0
		Me:                   me,
		peers:                peers,
		persister:            persister,
		State:                "",
		VotedFor:             -1,
	}

	// initialize from state persisted before a crash
	rf.readPersist(false)

	go rf.run(applyCh)
	dbg.LogKVs("Initialized Raft server", []string{tagMake}, map[string]interface{}{"rf": rf})
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
	rf.mu.Lock()
	defer func() {
		dbg.LogKVs("Returning lock", []string{tagLock, tagStart}, map[string]interface{}{"rf": rf})
		rf.mu.Unlock()
	}()

	if rf.State == Leader {
		// Append entry to log
		entry := LogEntry{rf.CurrentTerm, command}
		dbg.LogKVs("Appending submitted entry to log", []string{tagConsensus, tagLeader, tagStart}, map[string]interface{}{"entry": entry, "rf": rf})
		rf.Log = append(rf.Log, entry)
		index := len(rf.Log) - 1
		rf.persist(true)
		return index, rf.CurrentTerm, true
	}
	dbg.LogKVs("Rejecting command because not leader", []string{tagConsensus, tagInactivity, tagStart}, map[string]interface{}{"rf": rf})
	return -1, rf.CurrentTerm, false
}

//------------------------------------------------------------------------------

// Private functions

// applyLogEntries periodically checks for any committed entries that have not
// yet been applied, and applies them. It runs indefinitely.
func (rf *Raft) applyLogEntries(applyCh chan ApplyMsg, killCh chan bool) {
	lastApplied := 0
	for {
		dbg.LogKVs("Grabbing lock", []string{tagLock, tagApplyLogEntries}, map[string]interface{}{"lastApplied": lastApplied, "rf": rf})
		rf.mu.Lock()
		dbg.LogKVsIf(lastApplied < rf.CommitIndex, "Applying log entries through commit index", "", []string{tagApplyLogEntries}, map[string]interface{}{"lastApplied": lastApplied, "rf": rf})
		applyMsgs := make([]ApplyMsg, rf.CommitIndex-lastApplied)
		i := 0
		for lastApplied < rf.CommitIndex {
			lastApplied++
			applyMsgs[i] = ApplyMsg{
				Index:   lastApplied,
				Command: rf.Log[lastApplied].Command,
			}
			i++
		}
		dbg.LogKVs("Returning lock", []string{tagLock, tagApplyLogEntries}, map[string]interface{}{"lastApplied": lastApplied, "rf": rf})
		rf.mu.Unlock()

		for i := 0; i < len(applyMsgs); i++ {
			applyCh <- applyMsgs[i]
		}

		timeout := time.NewTimer(time.Millisecond * time.Duration(ApplyInterval))
		select {
		case <-timeout.C:
			break
		case <-killCh:
			return
		}
	}
}

// beCandidate runs the Candidate state for a particular term. Returns the next
// state to transition to, along with any params for that state.
func (rf *Raft) beCandidate() (State, Params) {
	// Set up Candidate
	dbg.LogKVs("Selecting lock or Convert To Follower signal", []string{tagBeCandidate, tagCandidate, tagElection, tagLock}, map[string]interface{}{"rf": rf})
	unlockCh, fparams := rf.selectLockOrConvertToFollowerSig()
	if unlockCh == nil {
		dbg.LogKVs("Received Convert To Follower signal", []string{tagBeCandidate, tagCandidate, tagElection, tagSignal}, map[string]interface{}{"rf": rf, "fparams": fparams})
		return Follower, Params{follower: fparams}
	}

	me := rf.Me
	npeers := len(rf.peers)
	rf.State = Candidate
	rf.CurrentTerm++
	currentTerm := rf.CurrentTerm
	rf.VotedFor = rf.Me
	numVotes := 1

	rf.persist(true)

	dbg.LogKVs("Entered Candidate state", []string{tagBeCandidate, tagCandidate, tagElection}, map[string]interface{}{"rf": rf})

	// Send RequestVote RPCs
	dbg.LogKVs("Sending round of RequestVotes", []string{tagBeCandidate, tagCandidate, tagElection}, map[string]interface{}{"rf": rf})
	lastLogIndex := len(rf.Log) - 1
	args := RequestVoteArgs{me, lastLogIndex, rf.Log[lastLogIndex].Term, currentTerm}
	votesCh := make(chan bool, npeers)
	for i := 0; i < npeers; i++ {
		if i == me {
			continue
		}

		go func(i int, args RequestVoteArgs) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, &reply)
			if !ok {
				return // ignore failed RPCs
			}

			dbg.LogKVs("Grabbing lock", []string{tagBeCandidate, tagCandidate, tagElection, tagLock}, map[string]interface{}{"rf": rf})
			rf.mu.Lock()
			if reply.Term > rf.CurrentTerm {
				dbg.LogKVs("Sending Convert To Follower signal", []string{tagBeCandidate, tagCandidate, tagElection, tagSignal}, map[string]interface{}{"args": args, "i": i, "reply": reply, "rf": rf})
				rf.sendConvertToFollowerSig(reply.Term, true)

				dbg.LogKVs("Returning lock", []string{tagBeCandidate, tagCandidate, tagElection, tagLock}, map[string]interface{}{"rf": rf})
				rf.mu.Unlock()
				return
			}
			dbg.LogKVs("Returning lock", []string{tagBeCandidate, tagCandidate, tagElection, tagLock}, map[string]interface{}{"rf": rf})
			rf.mu.Unlock()

			if reply.VoteGranted {
				votesCh <- true
			}
		}(i, args)
	}

	dbg.LogKVs("Returning lock", []string{tagBeCandidate, tagCandidate, tagElection, tagLock}, map[string]interface{}{"rf": rf})
	unlockCh <- true

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
	select {
	case <-winSig:
		dbg.LogKVs("Received Win signal", []string{tagBeCandidate, tagCandidate, tagElection, tagSignal}, map[string]interface{}{"rf": rf})
		lparams := LeaderParams{currentTerm: currentTerm, nextIndex: nil, matchIndex: nil}
		return Leader, Params{leader: lparams}
	case fparams := <-rf.convertToFollowerSig:
		dbg.LogKVs("Received Convert To Follower signal", []string{tagBeCandidate, tagCandidate, tagElection, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "fparams": fparams})
		return Follower, Params{follower: fparams}
	case <-electionTimer.C:
		dbg.LogKVs("Election timer timed out", []string{tagBeCandidate, tagCandidate, tagElection, tagSignal}, map[string]interface{}{"rf": rf})
		return Candidate, Params{}
	}
}

// beFollower runs the Follower state for a particular term. Returns the next
// state to transition to and params for that state. newTerm is either a later
// term if one is known at the time of calling, or the current term if not.
// isLockHeld indicates whether the call is currently holding a lock. Send true
// on ackCh when the conversion to the Follower state is complete.
func (rf *Raft) beFollower(newTerm int, isLockHeld bool, ackCh chan bool) (State, Params) {
	// Set up Follower
	var unlockCh chan bool
	if !isLockHeld {
		dbg.LogKVs("Selecting lock or Convert To Follower signal", []string{tagBeFollower, tagFollower, tagLock}, map[string]interface{}{"newTerm": newTerm, "rf": rf})
		var fparams FollowerParams
		unlockCh, fparams = rf.selectLockOrConvertToFollowerSig()
		if unlockCh == nil {
			dbg.LogKVs("Received Convert To Follower signal", []string{tagBeFollower, tagFollower, tagInactivity, tagSignal}, map[string]interface{}{"newTerm": newTerm, "rf": rf, "fparams": fparams})
			return Follower, Params{follower: fparams}
		}
	}

	rf.State = Follower
	updatedTerm := false
	if newTerm > rf.CurrentTerm {
		rf.CurrentTerm = newTerm
		rf.VotedFor = -1
		updatedTerm = true

		rf.persist(true)
	}

	if !isLockHeld {
		dbg.LogKVs("Returning lock", []string{tagBeFollower, tagFollower, tagLock}, map[string]interface{}{"rf": rf, "unlockCh": unlockCh})
		unlockCh <- true
	}

	dbg.LogKVsIf(updatedTerm, "Entered Follower state in a new term", "Re-entered Follower state in same term", []string{tagBeFollower, tagFollower, tagInactivity}, map[string]interface{}{"isLockHeld": isLockHeld, "newTerm": newTerm, "rf": rf})
	ackCh <- true

	electionTimer := makeRandomTimer()

	// Wait for signals to decide next state
	select {
	case <-electionTimer.C:
		dbg.LogKVs("Election timer timed out", []string{tagBeFollower, tagFollower, tagSignal}, map[string]interface{}{"rf": rf})
		return Candidate, Params{}
	case fparams := <-rf.convertToFollowerSig:
		dbg.LogKVs("Received Convert To Follower signal", []string{tagBeFollower, tagFollower, tagInactivity, tagSignal}, map[string]interface{}{"rf": rf, "fparams": fparams})
		return Follower, Params{follower: fparams}
	}
}

// beLeader runs the Leader state for a particular term, indicated by
// currentTerm. Returns the next state to transition to, along with any params
// for that state. nextIndex and matchIndex are as specified by the paper. If
// this leader was just newly elected, pass nil for these values. Otherwise,
// pass the nextIndex and matchIndex from the last periodic interval.
func (rf *Raft) beLeader(currentTerm int, nextIndex []int, matchIndex []int) (State, Params) {
	dbg.LogKVs("Selecting lock or Convert To Follower signal", []string{tagBeLeader, tagLeader, tagLock}, map[string]interface{}{"currentTerm": currentTerm, "matchIndex": matchIndex, "nextIndex": nextIndex, "rf": rf})
	unlockCh, fparams := rf.selectLockOrConvertToFollowerSig()
	if unlockCh == nil {
		dbg.LogKVs("Received Convert To Follower signal", []string{tagBeLeader, tagLeader, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "fparams": fparams})
		return Follower, Params{follower: fparams}
	}

	// Set up Leader
	me := rf.Me
	rf.State = Leader

	newlyElected := (nextIndex == nil)
	if newlyElected {
		// Initialize nextIndex and matchIndex
		nextIndex = make([]int, len(rf.peers))
		matchIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			if i == me {
				continue
			}

			nextIndex[i] = len(rf.Log)
			matchIndex[i] = 0
		}
	}

	dbg.LogKVsIf(newlyElected, "Entered Leader state, newly elected", "", []string{tagBeLeader, tagConsensus, tagLeader}, map[string]interface{}{"currentTerm": currentTerm, "matchIndex": matchIndex, "nextIndex": nextIndex, "rf": rf})
	dbg.LogKVsIf(!newlyElected, "Re-entered Leader state after periodic interval", "", []string{tagBeLeader, tagInactivity, tagLeader}, map[string]interface{}{"currentTerm": currentTerm, "matchIndex": matchIndex, "nextIndex": nextIndex, "rf": rf})

	// Send a round of AppendEntries
	dbg.LogKVs("Sending round of AppendEntries", []string{tagBeLeader, tagInactivity, tagLeader}, map[string]interface{}{"rf": rf})
	for i := range rf.peers {
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
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, args, &reply)
			if !ok {
				return // ignore, try again next interval
			}

			dbg.LogKVs("Grabbing lock", []string{tagBeLeader, tagLeader, tagLock}, map[string]interface{}{"rf": rf})
			rf.mu.Lock()
			defer func() {
				dbg.LogKVs("Returning lock", []string{tagBeLeader, tagLeader, tagLock}, map[string]interface{}{"rf": rf})
				rf.mu.Unlock()
			}()

			if reply.Term > rf.CurrentTerm {
				dbg.LogKVs("Sending Convert To Follower signal", []string{tagBeLeader, tagConsensus, tagLeader, tagSignal}, map[string]interface{}{"args": args, "reply": reply, "rf": rf})
				rf.sendConvertToFollowerSig(reply.Term, true)
				return
			}

			if !reply.Success {
				nextIndex[i] = min(nextIndex[i], reply.NextIndex)
				return
			}

			matchIndex[i] = max(matchIndex[i], args.PrevLogIndex+len(args.Entries))
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
		if count >= (len(rf.peers)/2)+1 {
			dbg.LogKVs("Updating commit index", []string{tagBeLeader, tagConsensus, tagLeader}, map[string]interface{}{"count": count, "currentTerm": currentTerm, "i": i, "matchIndex": matchIndex, "nextIndex": nextIndex, "rf": rf})
			rf.CommitIndex = i
			break
		}
	}

	dbg.LogKVs("Returning lock", []string{tagBeLeader, tagLeader, tagLock}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf})
	unlockCh <- true

	periodicTimer := time.NewTimer(time.Millisecond * time.Duration(LeaderPeriodicInterval))

	// Wait for signals to determine next state
	select {
	case fparams := <-rf.convertToFollowerSig:
		dbg.LogKVs("Received Convert To Follower signal", []string{tagBeLeader, tagLeader, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "fparams": fparams})
		return Follower, Params{follower: fparams}
	case <-periodicTimer.C:
		dbg.LogKVs("Leader periodic interval timed out", []string{tagBeLeader, tagInactivity, tagLeader, tagSignal}, map[string]interface{}{"rf": rf})
		lparams := LeaderParams{currentTerm: currentTerm, nextIndex: nextIndex, matchIndex: matchIndex}
		return Leader, Params{leader: lparams}
	}
}

// max returns the max of two numbers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// min returns the min of two numbers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// makeRandomTimer returns a timer of random duration.
func makeRandomTimer() *time.Timer {
	r := RandomMinInterval + rand.Intn(RandomMaxInterval-RandomMinInterval)
	return time.NewTimer(time.Millisecond * time.Duration(r))
}

// persist saves Raft's persistent state to stable storage, where it can later
// be retrieved after a crash and restart. Pass isLockHeld, which indicates
// whether the caller is currently holding a lock.
//
// See the paper's Figure 2 for a description of what should be persisent.
func (rf *Raft) persist(isLockHeld bool) {
	if !isLockHeld {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	w := &bytes.Buffer{}
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// readPersist restores previously persisted state. isLockHeld indicates whether
// the caller is currently holding a lock.
func (rf *Raft) readPersist(isLockHeld bool) {
	if !isLockHeld {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	data := rf.persister.ReadRaftState()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Log)
}

// run is the main continuous process that runs the Raft server. It starts the
// server in the Follower state, and manages the transitions between the
// different states.
func (rf *Raft) run(applyCh chan ApplyMsg) {
	killApplyLogEntriesCh := make(chan bool, 1)
	go rf.applyLogEntries(applyCh, killApplyLogEntriesCh)

	nextState := Follower
	params := Params{follower: FollowerParams{
		NewTerm:    0,
		IsLockHeld: false,
		ackCh:      make(chan bool, 1),
	}}
	for {
		switch nextState {
		case Follower:
			nextState, params = rf.beFollower(params.follower.NewTerm, params.follower.IsLockHeld, params.follower.ackCh)
		case Candidate:
			nextState, params = rf.beCandidate()
		case Leader:
			nextState, params = rf.beLeader(params.leader.currentTerm, params.leader.nextIndex, params.leader.matchIndex)
		}

		// Check if server was killed
		select {
		case <-rf.killCh:
			killApplyLogEntriesCh <- true
			return
		default:
			break
		}
	}
}

// selectLockOrConvertToFollowerSig simulates a "select" statement between
// grabbing a lock or receiving a Convert To Follower signal. Returns a
// channel if we get the lock first, or the signal if we get it first. If you
// get the channel, send true on it to return the lock. The unlock channel will
// be nil if you don't get the lock, so you can use this to determine which
// value you got.
func (rf *Raft) selectLockOrConvertToFollowerSig() (chan bool, FollowerParams) {
	// Convert request for a lock into a channel send, so we can select on it.
	// Get the lock by reading from lockCh, and cancel the request for the lock
	// or return it by sending to cancelorUnlockCh.
	lockCh := make(chan bool, 1)
	cancelOrUnlockCh := make(chan bool, 1)
	go func() {
		rf.mu.Lock()
		lockCh <- true
		<-cancelOrUnlockCh
		rf.mu.Unlock()
	}()

	// Try to grab lock, but cancel if we receive a Convert To Follower signal
	select {
	case <-lockCh:
		return cancelOrUnlockCh, FollowerParams{}
	case fparams := <-rf.convertToFollowerSig:
		cancelOrUnlockCh <- true
		return nil, fparams
	}
}

// sendAppendEntries is a wrapper for sending an AppendEntries RPC to `server`.
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	okCh := make(chan bool, 1)
	go func() {
		okCh <- rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
func (rf *Raft) sendConvertToFollowerSig(newTerm int, isLockHeld bool) {
	ackCh := make(chan bool, 1)
	rf.convertToFollowerSig <- FollowerParams{newTerm, isLockHeld, ackCh}
	<-ackCh
}

// sendRequestVote is a wrapper for sending a RequestVote RPC to `server`.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	okCh := make(chan bool, 1)
	go func() {
		okCh <- rf.peers[server].Call("Raft.RequestVote", args, reply)
	}()

	timeout := time.NewTimer(time.Millisecond * time.Duration(RPCTimeoutInterval))
	select {
	case ok := <-okCh:
		return ok
	case <-timeout.C:
		return false
	}
}
