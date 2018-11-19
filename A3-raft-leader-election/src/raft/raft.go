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
	"dbg"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//------------------------------------------------------------------------------

// Constants

// State is a state of the Raft.
type State string

const (
	Follower  State = "Follower"
	Candidate State = "Candidate"
	Leader    State = "Leader"
)

// Log tags
const (
	tagCandidate = "candidate"
	tagFollower  = "follower"
	tagHeartbeat = "heartbeat"
	tagLeader    = "leader"
	tagLock      = "lock"
	tagNewState  = "newState"
	tagSignal    = "signal"
)

//------------------------------------------------------------------------------

// Types

// AppendEntriesArgs contains the arguments for AppendEntries.
type AppendEntriesArgs struct {
	Term int
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

// Raft implements a Raft server.
type Raft struct {
	currentTerm int
	mu          sync.Mutex
	me          int // index into peers[]
	peers       []*labrpc.ClientEnd
	persister   *Persister
	resetSig    chan ResetSignal
	state       State // Follower, Candidate, or Leader
	stepDownSig chan StepDownSignal
	votedFor    int
}

// RequestVoteArgs contains the arguments for RequestVote.
type RequestVoteArgs struct {
	CandidateID int
	Term        int
}

// RequestVoteReply contains the reply for RequestVote.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// ResetSignal represents a Reset signal.
type ResetSignal struct {
	currentTerm int
	newTerm     int
}

// StepDownSignal represents a StepDown signal.
type StepDownSignal struct {
	currentTerm int
	newTerm     int
}

//------------------------------------------------------------------------------

// RPCs

// AppendEntries is the handler for receiving a AppendEntries RPC.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	reply.Success = true

	// Decide how to signal based on state
	switch rf.state {
	case Follower:
		if args.Term > rf.currentTerm {
			rf.resetSig <- ResetSignal{rf.currentTerm, args.Term}
		} else {
			rf.resetSig <- ResetSignal{rf.currentTerm, -1}
		}
	case Candidate:
		if args.Term > rf.currentTerm {
			rf.stepDownSig <- StepDownSignal{rf.currentTerm, args.Term}
		} else {
			rf.stepDownSig <- StepDownSignal{rf.currentTerm, -1}
		}
	case Leader:
		if args.Term > rf.currentTerm {
			rf.stepDownSig <- StepDownSignal{rf.currentTerm, args.Term}
		}
	}
}

// RequestVote is the handler for receiving a RequestVote RPC.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Always send back my term, and default to not voting
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	// Grant vote if I haven't voted yet (or if it's a retry from same candidate)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	}

	// Possibly signal based on term and state
	if args.Term > rf.currentTerm {
		if rf.state == Candidate || rf.state == Leader {
			rf.stepDownSig <- StepDownSignal{rf.currentTerm, args.Term}
		}
	}
}

//------------------------------------------------------------------------------

// Exported functions

// GetState returns currentTerm and whether this server believes it is the
// leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	isLeader := (rf.state == Leader)
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
		currentTerm: -1,
		me:          me,
		peers:       peers,
		persister:   persister,
		resetSig:    make(chan ResetSignal, len(peers)),
		state:       "",
		stepDownSig: make(chan StepDownSignal, len(peers)),
		votedFor:    -1,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Enter Follower state
	go rf.beFollower(0)

	return rf
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	currentTerm := rf.currentTerm
	isLeader := (rf.state == Leader)
	return index, currentTerm, isLeader
}

//------------------------------------------------------------------------------

// Private functions

// beFollower runs the Follower state for a particular term. Optionally pass a
// later term as newTerm; if there is no new term, pass -1 and we'll use the
// current term.
func (rf *Raft) beFollower(newTerm int) {
	// Set up Follower
	dbg.LogKVs("Grabbing lock", []string{tagFollower, tagLock}, map[string]interface{}{"rf": rf})
	rf.mu.Lock()
	rf.state = Follower
	if newTerm != -1 {
		rf.currentTerm = newTerm
		rf.votedFor = -1
	}
	currentTerm := rf.currentTerm
	dbg.LogKVs("Returning lock", []string{tagFollower, tagLock}, map[string]interface{}{"rf": rf})
	rf.mu.Unlock()

	dbg.LogKVsIf(newTerm != -1, "Entered Follower state in a new term", "Re-entered Follower state in same term", []string{tagFollower, tagNewState}, map[string]interface{}{"rf": rf})

	electionTimer := makeRandomTimer()

	// Wait for signals to decide next state
	done := false
	for !done {
		select {
		case <-electionTimer.C:
			dbg.LogKVs("Election timer timed out", []string{tagFollower, tagSignal}, map[string]interface{}{"rf": rf})
			go rf.beCandidate()
			done = true
		case sig := <-rf.resetSig:
			if sig.currentTerm != currentTerm {
				dbg.LogKVs("Received Reset signal, ignoring because term is wrong", []string{tagFollower, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "sig": sig})
				continue
			}

			dbg.LogKVs("Received Reset signal, valid", []string{tagFollower, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "sig": sig})
			if !electionTimer.Stop() {
				<-electionTimer.C
			}
			go rf.beFollower(sig.newTerm)
			done = true
		}
	}
}

// beCandidate runs the Candidate state for a particular term.
func (rf *Raft) beCandidate() {
	// Set up Candidate
	dbg.LogKVs("Grabbing lock", []string{tagCandidate, tagLock}, map[string]interface{}{"rf": rf})
	rf.mu.Lock()
	me := rf.me
	rf.state = Candidate
	rf.currentTerm++
	currentTerm := rf.currentTerm
	rf.votedFor = rf.me
	numVotes := 1

	// Construct args/replies for RequestVote RPCs
	args := make([]RequestVoteArgs, len(rf.peers))
	replies := make([]RequestVoteReply, len(rf.peers))
	for i, _ := range rf.peers {
		args[i] = RequestVoteArgs{me, currentTerm}
		replies[i] = RequestVoteReply{}
	}
	dbg.LogKVs("Returning lock", []string{tagCandidate, tagLock}, map[string]interface{}{"rf": rf})
	rf.mu.Unlock()

	dbg.LogKVs("Entered Candidate state", []string{tagCandidate, tagNewState}, map[string]interface{}{"rf": rf})

	// Send RequestVote RPCs
	votesCh := make(chan bool, len(args))
	for i := 0; i < len(args); i++ {
		if i == me {
			continue
		}

		go func(i int) {
			dbg.LogKVs("Sending RequestVote", []string{tagCandidate}, map[string]interface{}{"i": i, "args[i]": args[i], "rf": rf})
			ok := rf.sendRequestVote(i, args[i], &replies[i])
			if !ok {
				return // ignore failed RPCs
			}

			if replies[i].Term > currentTerm {
				sig := StepDownSignal{currentTerm, replies[i].Term}
				dbg.LogKVs("Sending Step Down signal", []string{tagCandidate, tagSignal}, map[string]interface{}{"rf": rf, "sig": sig})
				rf.stepDownSig <- sig
				return
			}

			if replies[i].VoteGranted {
				dbg.LogKVs("Got RequestVote reply", []string{tagCandidate}, map[string]interface{}{"i": i, "replies[i]": replies[i], "rf": rf})
				votesCh <- true
			}
		}(i)
	}

	// Tally votes in the background
	electionTimer := makeRandomTimer()
	winSig := make(chan bool, 1)
	go func() {
		for i := 0; i < len(args); i++ {
			<-votesCh
			numVotes++
			if numVotes == (len(args)/2)+1 {
				dbg.LogKVs("Sending Win signal", []string{tagCandidate, tagSignal}, map[string]interface{}{"numVotes": numVotes, "rf": rf})
				winSig <- true
			}
		}
	}()

	// Wait for signals to decide next state
	done := false
	for !done {
		select {
		case <-winSig:
			dbg.LogKVs("Received Win signal", []string{tagCandidate, tagSignal}, map[string]interface{}{"rf": rf})
			go rf.beLeader()
			done = true
		case sig := <-rf.stepDownSig:
			if sig.currentTerm != currentTerm {
				dbg.LogKVs("Received Step Down signal, ignoring because term is wrong", []string{tagCandidate, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "sig": sig})
				continue
			}

			dbg.LogKVs("Received Step Down signal, valid", []string{tagCandidate, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "sig": sig})
			go rf.beFollower(sig.newTerm)
			done = true
		case <-electionTimer.C:
			dbg.LogKVs("Election timer timed out", []string{tagCandidate, tagSignal}, map[string]interface{}{"rf": rf})
			if !electionTimer.Stop() {
				<-electionTimer.C
			}
			go rf.beCandidate()
			done = true
		}
	}
}

// beLeader runs the Leader state for a particular term.
func (rf *Raft) beLeader() {
	// Set up Leader
	dbg.LogKVs("Grabbing lock", []string{tagCandidate, tagLock}, map[string]interface{}{"rf": rf})
	rf.mu.Lock()
	me := rf.me
	rf.state = Leader
	currentTerm := rf.currentTerm

	// Construct heartbeat args/replies
	args := make([]AppendEntriesArgs, len(rf.peers))
	replies := make([]AppendEntriesReply, len(rf.peers))
	for i, _ := range rf.peers {
		args[i] = AppendEntriesArgs{currentTerm}
		replies[i] = AppendEntriesReply{}
	}
	dbg.LogKVs("Returning lock", []string{tagCandidate, tagLock}, map[string]interface{}{"rf": rf})
	rf.mu.Unlock()

	dbg.LogKVs("Entered Leader state", []string{tagLeader, tagNewState}, map[string]interface{}{"rf": rf})

	// Send a round of heartbeats
	for i := 0; i < len(args); i++ {
		if i == me {
			continue
		}

		go func(i int) {
			dbg.LogKVs("Sending heartbeat", []string{tagLeader, tagHeartbeat}, map[string]interface{}{"i": i, "args[i]": args[i], "rf": rf})
			ok := rf.sendAppendEntries(i, args[i], &replies[i])
			if !ok {
				return // ignore failed RPCs
			}

			if replies[i].Term > currentTerm {
				sig := StepDownSignal{currentTerm, replies[i].Term}
				dbg.LogKVs("Sending Step Down signal", []string{tagLeader, tagSignal}, map[string]interface{}{"rf": rf, "sig": sig})
				rf.stepDownSig <- sig
				return
			}
		}(i)
	}

	heartbeatTimer := newHeartbeatTimer()

	// Wait for signals to determine next state
	done := false
	for !done {
		select {
		case sig := <-rf.stepDownSig:
			if sig.currentTerm != currentTerm {
				dbg.LogKVs("Received Step Down signal, ignoring because term is wrong", []string{tagLeader, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "sig": sig})
				continue
			}

			dbg.LogKVs("Received Step Down signal, valid", []string{tagLeader, tagSignal}, map[string]interface{}{"currentTerm": currentTerm, "rf": rf, "sig": sig})
			go rf.beFollower(sig.newTerm)
			done = true
		case <-heartbeatTimer.C:
			dbg.LogKVs("Heartbeat timer timed out", []string{tagLeader, tagSignal}, map[string]interface{}{"rf": rf})
			go rf.beLeader()
			done = true
		}
	}
}

// newHeartbeatTimer returns a timer of fixed duration - the interval between
// heartbeat messages sent by a Leader.
func newHeartbeatTimer() *time.Timer {
	return time.NewTimer(time.Millisecond * time.Duration(75))
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
	minMillisecs := 150
	maxMillisecs := 300

	randMillisecs := minMillisecs + rand.Intn(maxMillisecs-minMillisecs)
	return time.NewTimer(time.Millisecond * time.Duration(randMillisecs))
}

// sendAppendEntries is a wrapper for sending an AppendEntries RPC to `server`.
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// sendRequestVote is a wrapper for sending a RequestVote RPC to `server`.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
