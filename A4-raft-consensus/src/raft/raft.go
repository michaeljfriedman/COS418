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

import "fmt"
import "labrpc"
import "log"
import "math/rand"
import "sync"
import "time"

// import "bytes"
// import "encoding/gob"

//------------------------------------------------------------------------------

// Debugging tools

// Set this debug flag to true to print out useful log statements during
// leader election.
const debugElection = true

// Set this debug flag to true for log statements during consensus
const debugConsensus = true

// Debugging streams
const ElectionStream = "Election"
const ConsensusStream = "Consensus"
const TestStream = "Test"

//
// Writes msg to the debug log on the stream `stream`. (Pass
// fmt.Sprintf("...") as s to print a formatted string)
//
func debugln(stream string, msg string) {
	switch stream {
	case ElectionStream:
		if debugElection {
			log.Printf("[%v] %v\n", ElectionStream, msg)
		}
		break
	case ConsensusStream:
		if debugConsensus {
			log.Printf("[%v] %v\n", ConsensusStream, msg)
		}
		break
	default:
		log.Printf("[%v] %v\n", stream, msg)
	}
}


//
// Returns this server's log in string form. Format of each log entry is
// index:term, with a pipe after the last committed entry. e.g.
//   [ 1:1 2:1 3:1 | 4:2 5:3 ]
//
// NOTE: To use this, you will need to surround it with locks yourself (
// I don't lock inside the function, in case you want to use it in a place
// already locked). e.g.
//   rf.mu.Lock()
// 	 debugln(ConsensusStream, rf.logToStream())
//   rf.mu.Unlock()
func (rf *Raft) logToString() string {
	s := "[ "
	for i := 1; i < len(rf.log); i++ {
		s += fmt.Sprintf("%v:%v ", rf.log[i].Index, rf.log[i].Term)
		if rf.log[i].Index == rf.commitIndex {
			s += "| "
		}
	}
	s += "]"

	return s
}


//------------------------------------------------------------------------------

// Constants

// Leader statuses
const Leader = 0
const Candidate = 1
const Follower = 2

// Election outcomes (relative to this server)
const Won = 0
const Lost = 1
const Timeout = 2
const Stale = 3

// Set votedFor to NoOne to indicate no vote granted yet in an
// election
const NoOne = -1

// Consensus outcomes. "Stale" (defined above) is also a valid value.
const Success = 0

// AppendEntries reply statuses. "Success" and "Stale" (defined above) are also
// valid values.
//
// ("Stale" in this context indicates that the term the AppendEntries message
// was outdated on receipt.)
const Failure = 1
const WasntReady = 2  // indicates recipient was not a follower when AppendEntries was sent. Try again to append log entries.

//------------------------------------------------------------------------------

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Many of these attributes are from the paper; most of them are uncommented
	// since they are documented in section 5. I comment the ones I added.

	// General attributes
	currentTerm   int

	// Attributes for elections
	votedFor        int         // server id, or NoOne
	leaderStatus    int         // Leader, Candidate, or Follower
	heartbeatTimer  *time.Timer // time until I consider leader dead
	electionTimer   *time.Timer // time until I restart an election
	electionOutcome chan int    // Won, Lost, Timeout, or Stale. Use chan so
	                            // reading will block until an election has ended
	numVotes        int

	// Attributes for logging/consensus
	log           []*LogEntry
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	applyCh       chan ApplyMsg // send log entries to this channel to "apply" them

	// DEBUG
	// Attributes for help with debugging
	appendEntriesId int    // counter for message IDs
	logString       string // update whenever entries are appended or committed
}

//
// A log entry
//
type LogEntry struct {
	// Attributes from paper
	Command interface{}
	Term    int

	// Extra attributes
	Index            int      // index of this entry in the log
	NumReplications  int      // num servers that replicated this entry (<= majority)
	IsPendingConsensus bool   // is this entry pending consensus?
	ConsensusOutcome chan int // Success or Stale. Use chan so reading will block
	                          // until consensus has been reached
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = (rf.leaderStatus == Leader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}


//------------------------------------------------------------------------------

// Leader election (data types/functions)

//
// Helper function. Generates and returns a timer with a random time from
// minMillisecs to maxMillisecs.
//
// If you want to use a timer, you should not call this method. Instead,
// call one of the specific methods for the timer you're trying to create (see
// functions below).
//
func randomTimer(minMillisecs int, maxMillisecs int) *time.Timer {
	randMillisecs := minMillisecs + rand.Intn(maxMillisecs - minMillisecs)
	return time.NewTimer(time.Millisecond * time.Duration(randMillisecs))
}

//
// Returns a random timer for duration of an election (i.e. how long a
// server waits for heartbeats or votes before starting a new election)
//
func electionTimer() *time.Timer {
	return randomTimer(150, 300)
}

//
// Returns a timer for duration between two rounds of heartbeats (for leader).
//
func leaderHeartbeatTimer() *time.Timer {
	d := 30
	return time.NewTimer(time.Millisecond * time.Duration(d))
}

//
// Returns the term of the last log entry, or 0 if there are no log entries.
//
func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 1 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

//
// Helper function. Returns the number of servers that makes exactly a
// majority.
//
func (rf *Raft) majority() int {
	return (len(rf.peers) / 2) + 1
}

//
// Start a timer for heartbeat messages from the leader. When the timer expires,
// assume the leader is dead, and run for the new leader if I have not yet
// voted for a new one.
//
// Outside this function, the timer should be stopped and this function should
// be called again when a heartbeat is received.
//
func (rf *Raft) waitForLeaderToDie() {
	rf.heartbeatTimer = electionTimer()

	<-rf.heartbeatTimer.C

	debugln(ElectionStream, fmt.Sprintf("Term %d: %d wants to run. (votedFor = %d)", rf.currentTerm, rf.me, rf.votedFor))

	go rf.runForLeader()
}

//
// As leader, sends heartbeats periodically to other servers to let them
// know I'm still alive.
//
func (rf *Raft) sendPeriodicHeartbeats() {
	for rf.leaderStatus == Leader {
		debugln(ConsensusStream, fmt.Sprintf("Term %v: %v (leader) is sending round of heartbeats. (commitIndex = %v, nextIndex = %v, matchIndex = %v)", rf.currentTerm, rf.me, rf.commitIndex, rf.nextIndex, rf.matchIndex))

		for peerId, _ := range rf.peers {
			// Skip me
			if peerId == rf.me {
				continue
			}

			go rf.sendHeartbeatTo(peerId)
		}

		timer := leaderHeartbeatTimer()
		<-timer.C
	}
}



//
// example RequestVote RPC arguments structure.
// See paper section 5.2 for reference
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// See paper section 5.2 for reference
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// Determines whether I think a candidate is eligible to run for leader given
// their RequestVoteArgs. Returns true/false if eligible/not eligible.
//
// A server is eligible if its log is as "up to date" as mine (see paper
// section 5.4.1 for details).
//
func (rf *Raft) canBeLeader(args RequestVoteArgs) bool {
	// Does candiate's log have a later term than mine? If so, their log is
	// more up to date.
	if args.LastLogTerm > rf.lastLogTerm() {
		return true
	}

	// If terms are equal, then their log is more up to date if it's longer.
	if args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= len(rf.log)-1 {
		return true
	}

	// Neither test passed. Candidate's log must not be as up to date
	return false
}

//
// Handles a RequestVote message from a candidate. I grant a vote for the
// candidate if they're running in a newer election than I am (if I also think
// I'm a candidate), or if they are eligible to run (see canBeLeader()) and
// if I have not yet voted in this term.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()  // read/update currentTerm and votedFor atomically
	defer rf.mu.Unlock()

	//---------------
	// Base cases
	//---------------

	if args.Term < rf.currentTerm {
		// You're running in an outdated election
		reply.VoteGranted = false
		return
	}

	// Make sure to update my term to latest term, at the end
	defer func() {
		rf.currentTerm = args.Term
	}()

	if rf.leaderStatus == Candidate && args.Term > rf.currentTerm {
		// I'm running in an outdated election. I stop my outdated candidacy
		rf.electionOutcome <- Stale
	}

	if !rf.canBeLeader(args) {
		// You are not eligible to be leader. Sorry bud.
		reply.VoteGranted = false
		return
	}

	//-------------------------------
	// Decide whether to grant vote
	//-------------------------------

	if rf.votedFor == NoOne || args.Term > rf.currentTerm {
		// You (the caller) are eligible to run and I haven't voted yet (or it's
		// a new election), so I vote for you.
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		// I voted already in this term
		reply.VoteGranted = false
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// Request a vote from the given server and processs the reply.
//
func (rf *Raft) requestVoteFrom(server int) {
	// Request vote
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		len(rf.log)-1,    // last log index
		rf.lastLogTerm(), // last log term
	}
	reply := RequestVoteReply{
		rf.currentTerm,
		false,
	}
	ok := rf.sendRequestVote(server, args, &reply)

	if !ok {
		return
	}

	debugln(ElectionStream, fmt.Sprintf("Term %d: %d received vote reply from %d", rf.currentTerm, rf.me, server))

	// Process reply. If applicable, add a vote for me and check if I won
	if rf.leaderStatus == Candidate && reply.Term == rf.currentTerm && reply.VoteGranted {
		debugln(ElectionStream, fmt.Sprintf("Term %d: %d was voted for by %d", rf.currentTerm, rf.me, server))

		rf.numVotes++
		if rf.numVotes == rf.majority() {
			rf.electionOutcome <- Won
		}
	} else {
		debugln(ElectionStream, fmt.Sprintf("Term %d: %d did not use vote reply from or was denied by %d", rf.currentTerm, rf.me, server))
	}
}


//
// Sends a heartbeat message to the specified server. Should only be called
// by the leader.
//
func (rf *Raft) sendHeartbeatTo(server int) {
	rf.sendAppendEntries(server)
}


//
// Start a new election, and run for leader.
// See paper section 5.2 for reference.
//
func (rf *Raft) runForLeader() {
	// Initiate my election *atomically*, if I can, or cancel if someone has
	// already started running before me. `proceed` indicates whether I can
	// proceed with my election.
	proceed := func() bool {
		rf.mu.Lock()  // need to vote for myself atomically
		defer rf.mu.Unlock()

		// Check if, to my knowledge, anyone has already started running before me
		if rf.votedFor != NoOne {
			return false
		}

		// Special case - if I'm the only server, I just become leader
		if len(rf.peers) == 1 {
			rf.currentTerm += 1
			rf.leaderStatus = Leader
			return false
		}

		// Initiate my election
		rf.currentTerm += 1
		rf.leaderStatus = Candidate
		rf.numVotes = 0

		// Vote for myself
		rf.numVotes++
		rf.votedFor = rf.me

		debugln(ElectionStream, fmt.Sprintf("Term %d: %d voted for himself", rf.currentTerm, rf.me))

		return true
	}()

	if !proceed {
		debugln(ElectionStream, fmt.Sprintf("Term %v: %v couldn't run. Election started already (votedFor = %v)", rf.currentTerm, rf.me, rf.votedFor))
		return
	}

	// Start a timer for the election. If the timer expires, the election "timed
	// out" (i.e. no winner)
	rf.electionTimer = electionTimer()
	go func() {
		<-rf.electionTimer.C
		rf.electionOutcome <- Timeout
	}()

	//-------------------------------------

	// Request votes from each other server
	for peerId, _ := range rf.peers {
		// Skip me
		if peerId == rf.me {
			continue
		}

		// Request the vote. Use separate goroutine for each server
		go rf.requestVoteFrom(peerId)
	}

	debugln(ElectionStream, fmt.Sprintf("Term %d: %d requested votes from other servers", rf.currentTerm, rf.me))

	//-------------------------------------

	// Wait for and process election outcome (won, lost, or timeout)
	outcome := <-rf.electionOutcome
	if outcome == Won {
		// I become leader
		rf.electionTimer.Stop()
		rf.leaderStatus = Leader

		// Reset who I voted for
		rf.votedFor = NoOne

		// Initialize vars I need to keep track of as leader (see section 5.3)
		nextIndex := len(rf.log)  // last log index + 1
		for peerId, _ := range rf.peers {
			rf.nextIndex[peerId] = nextIndex
			rf.matchIndex[peerId] = 0
		}

		// Start sending periodic heartbeats to other servers to indicate
		// that I'm their new leader
		go rf.sendPeriodicHeartbeats()

		debugln(ConsensusStream, fmt.Sprintf("Term %v: %v is now leader", rf.currentTerm, rf.me))

		debugln(ElectionStream, fmt.Sprintf("Term %v: %v has won election and sent notifs to other servers", rf.currentTerm, rf.me))
	} else if outcome == Lost {
		// I become follower
		rf.electionTimer.Stop()
		rf.leaderStatus = Follower

		// Reset who I voted for
		rf.votedFor = NoOne
	} else if outcome == Timeout {
		// Restart election
		debugln(ElectionStream, fmt.Sprintf("Term %v: %v timed out election and is restarting", rf.currentTerm, rf.me))

		rf.votedFor = NoOne
		go rf.runForLeader()
	} else if outcome == Stale {
		// I realized I am running in an outdated election. Stop running.
		rf.electionTimer.Stop()
		rf.leaderStatus = Follower
	}
}

//------------------------------------------------------------------------------

// AppendEntries
// (Overlaps into both elections and consensus)

//
// An AppendEntries message (see paper section 5 for reference)
// Has two roles:
// - Leader tells followers to "append entries" to their logs
// - Leader sends heartbeats to followers
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []*LogEntry

	// DEBUG
	// Attributes for help with debugging
	Id int // unique id for each message sent by this server
}

//
// A reply to an AppendEntries message (see paper section 5 for reference)
//
// Quick overview:
// A reply with Success = true indicates that the log entry(s) sent in the
// corresponding Args are replicated/in sync on the replying server.
// Success = false indicates that they are not.
//
type AppendEntriesReply struct {
	Term    int
	Status  int  // Success, Failure, or WasntReady. Serves role of "success"
	             // attribute from the paper (see Figure 2)

	// DEBUG
	// Attributes for help with debugging
	Id int // same as corresponding arg id
}

//
// Delete entries in the log from index `index` to the end. Return the
// new log.
//
func (rf *Raft) deleteEntriesFrom(index int) []*LogEntry {
	return rf.log[:index]
}

//
// Returns true if this server's log matches the leader's up through
// prevLogIndex, given prevLogTerm as well. False if not.
//
func (rf *Raft) logsMatchThrough(prevLogIndex int, prevLogTerm int) bool {
	if prevLogIndex <= 0 { // base case 1
		return true
	} else if prevLogIndex > len(rf.log) - 1 { // base case 2
		return false
	} else {
		// Normal case
		return rf.log[prevLogIndex].Term == prevLogTerm
	}
}

//
// AppendEntries RPC handler. Processes a heartbeat. If I'm the leader,
// I step down to the caller (new leader). If I'm a candidate, I declare
// that I've lost the election to the caller (new leader). If I'm a follower,
// note that the leader is still alive, replicate my log, and reply.
//
// Note there is no reply unless I am a follower.
//
// (Ref paper figure 2 and section 5)
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Id = args.Id

	if args.Term < rf.currentTerm {
		// Message is from an old term
		reply.Status = Stale
		reply.Term = rf.currentTerm

		debugln(ConsensusStream, fmt.Sprintf("Term %v: %v replied to %v (leader) AE %v 'stale'", rf.currentTerm, rf.me, args.LeaderId, args.Id))

		return
	}

	//----------------------
	// Process message
	//----------------------

	// Start waiting for heartbeats again when you're done here
	defer func() {
		go rf.waitForLeaderToDie()
	}()

	// Update my term
	rf.currentTerm = args.Term

	if rf.leaderStatus == Leader {
		// I step down to new leader
		rf.leaderStatus = Follower
		rf.votedFor = NoOne

		// Clear nextIndex, matchIndex
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = 0
			rf.matchIndex[i] = 0
		}

		// Mark all pending consensus outcomes as "stale"
		for i := rf.commitIndex+1; i < len(rf.log); i++ {
			if rf.log[i].IsPendingConsensus {
				rf.log[i].ConsensusOutcome <- Stale
			}
		}

		// Reply "wasn't ready"
		reply.Status = WasntReady
		reply.Term = rf.currentTerm

		debugln(ConsensusStream, fmt.Sprintf("Term %v: %v replied to %v (leader) AE %v 'wasn't ready'", rf.currentTerm, rf.me, args.LeaderId, args.Id))
	} else if rf.leaderStatus == Candidate {
		// I lost
		rf.electionOutcome <- Lost

		// Reply "wasn't ready"
		reply.Status = WasntReady
		reply.Term = rf.currentTerm

		debugln(ConsensusStream, fmt.Sprintf("Term %v: %v replied to %v (leader) AE %v 'wasn't ready'", rf.currentTerm, rf.me, args.LeaderId, args.Id))
	} else if rf.leaderStatus == Follower {
		// Reset heartbeat timer for leader
		rf.heartbeatTimer.Stop()

		// Since this may also be the result of an election, reset who I voted for
		rf.votedFor = NoOne

		//--------------------------------------------
		// Replicate leader's log (or ask for more
		// entries if needed)
		//--------------------------------------------

		func() {
			// Replicate log *atomically*
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if !rf.logsMatchThrough(args.PrevLogIndex, args.PrevLogTerm) {
				// Logs out of sync. Ask for more entries
				reply.Status = Failure
				reply.Term = args.Term

				debugln(ConsensusStream, fmt.Sprintf("Term %v: %v replied to %v (leader) AE %v with failure", rf.currentTerm, rf.me, args.LeaderId, args.Id))

				return
			}

			// Delete conflicting entries
			firstNewEntry := 0                   // index into args.Entries
			startIndex := args.PrevLogIndex + 1  // index into rf.log
			if startIndex <= len(rf.log) - 1 {
				potentialConflicts := rf.log[startIndex:]
				if len(potentialConflicts) > len(args.Entries) {
					// I have extra entries. Delete them.
					rf.log = rf.deleteEntriesFrom(startIndex)
				} else {
					for i := 0; i < len(potentialConflicts); i++ {
						if potentialConflicts[i].Term != args.Entries[i].Term {
							// I have a conflict. Delete it and everything after it.
							rf.log = rf.deleteEntriesFrom(startIndex + i)
							firstNewEntry = i
							break
						}
					}
				}
			}

			// Append entries not in log
			for i := firstNewEntry; i < len(args.Entries); i++ {
				rf.log = append(rf.log, args.Entries[i])
				rf.logString = rf.logToString()  // DEBUG
			}


			//----------------------------------------------
			// Commit and apply new entries (if applicable)
			//----------------------------------------------

			if args.LeaderCommit > rf.commitIndex {
				// newCommitIndex = min(args.LeaderCommit, lastLogIndex)
				var newCommitIndex int
				lastLogIndex := len(rf.log) - 1
				if args.LeaderCommit < lastLogIndex {
					newCommitIndex = args.LeaderCommit
				} else {
					newCommitIndex = lastLogIndex
				}

				rf.applyLogEntries(newCommitIndex)
			}

			// Reply
			reply.Status = Success
			reply.Term = rf.currentTerm

			debugln(ConsensusStream, fmt.Sprintf("Term %v: %v replied to %v (leader) AE %v with success. (log = %v)", rf.currentTerm, rf.me, args.LeaderId, args.Id, rf.logString))
		}()
	}

}

//
// Sends an AppendEntries message to the provided server. This serves as both
// a heartbeat and an attempt to send necessary information to put the server's
// log in sync with the sender's (i.e. the leader's).
//
// By the time this terminates, it is *not* guaranteed that the recipient's log
// is in sync with the leader's - you may require another call. However, all
// parameters for the next call are set in the first call, so you can just call
// it again immediately if it's required. Also, as noted in the paper (section
// 5.5), these messages are idempotent, so there's no harm in calling this
// more times than is necessary.
//
func (rf *Raft) sendAppendEntries(server int) {
	// Make args for message. Send entries from nextIndex to end (or empty
	// if out of range)
	nextIndex := rf.nextIndex[server]
	var entries []*LogEntry
	if nextIndex <= len(rf.log) - 1 {
		entries = rf.log[nextIndex:]
	}

	prevLogIndex := nextIndex - 1
	var prevLogTerm int
	if prevLogIndex > 0 {
		prevLogTerm = rf.log[prevLogIndex].Term
	} else {
		prevLogTerm = -1  // no entry here, so no term
	}

	// Make args for message
	args := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		prevLogIndex,
		prevLogTerm,
		rf.commitIndex,
		entries,
		rf.appendEntriesId,
	}
	rf.appendEntriesId++

	// Initialize empty reply
	reply := &AppendEntriesReply{}

	if len(args.Entries) > 0 {
		debugln(ConsensusStream, fmt.Sprintf("Term: %v: %v (leader) sent AE %v to %v. (entries = [%v..%v], commitIndex = %v, prevLogIndex = %v)", rf.currentTerm, rf.me, args.Id, server, nextIndex, len(rf.log)-1, args.LeaderCommit, prevLogIndex))
	} else {
		debugln(ConsensusStream, fmt.Sprintf("Term: %v: %v (leader) sent AE %v to %v. (entries = [], commitIndex = %v, prevLogIndex = %v)", rf.currentTerm, rf.me, args.Id, server, args.LeaderCommit, prevLogIndex))
	}

	// Send message, get reply
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok || reply.Status == WasntReady {
		// Server never replied or wasn't a follower at time of sending. Just try
		// again in next heartbeat.

		debugln(ConsensusStream, fmt.Sprintf("Term %v: %v (leader) got inconclusive AE %v reply from %v", rf.currentTerm, rf.me, args.Id, server))

		return
	}

	// Process reply
	if reply.Status == Success {
		debugln(ConsensusStream, fmt.Sprintf("Term %v: %v (leader) got success AE %v reply from %v", rf.currentTerm, rf.me, reply.Id, server))

		// Reset nextIndex for that server
		rf.nextIndex[server] = len(rf.log)

		// Mark these entries replicated, if applicable
		for _, entry := range args.Entries {
			if entry.IsPendingConsensus && entry.Index > rf.matchIndex[server] /* not yet replicated on that server */ {
				entry.NumReplications++
				if entry.NumReplications == rf.majority() {
					entry.ConsensusOutcome <- Success
				}
			}

			rf.matchIndex[server] = entry.Index
		}

	} else if reply.Status == Failure {
		debugln(ConsensusStream, fmt.Sprintf("Term %v: %v (leader) got failure AE %v reply from %v", rf.currentTerm, rf.me, reply.Id, server))

		// Set nextIndex for next call
		rf.nextIndex[server]--
	} else if reply.Status == Stale {
		// The replier had a later term than me. Other servers will eventually
		// see this as well when the replier tries to start new elections. So a
		// new leader is coming. I don't have to do anything in the meantime.
		debugln(ConsensusStream, fmt.Sprintf("Term %v: %v (leader) got stale AE %v reply from %v", rf.currentTerm, rf.me, reply.Id, server))
	}
}


//------------------------------------------------------------------------------

// Logging/applying commands and leader-follower consensus

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Append to my log *atomically*
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Do not accept commands if not the leader
	if rf.leaderStatus != Leader {
		return -1, -1, false
	}

	// Create new LogEntry and append it
	newEntryIndex := rf.nextIndex[rf.me]
	newEntry := &LogEntry{
		command,
		rf.currentTerm,
		newEntryIndex,
		0,                 // NumReplications
		true,              // IsPendingConsensus
		make(chan int, 1), // ConsensusOutcome
	}
	rf.log = append(rf.log, newEntry)
	rf.logString = rf.logToString()  // DEBUG

	debugln(ConsensusStream, fmt.Sprintf("Term %v: %v (leader) added entry #%v to his log. (log = %v)", rf.currentTerm, rf.me, newEntry.Index, rf.logString))

	// Update index for the next log entry
	rf.nextIndex[rf.me] = newEntry.Index + 1
	rf.matchIndex[rf.me] = newEntry.Index

	// Start a consensus procedure on this entry
	go rf.getConsensus(newEntry)

	debugln(ConsensusStream, fmt.Sprintf("Term %v: %v (leader) received Start(). Returning index %v", rf.currentTerm, rf.me, newEntry.Index))

	return newEntry.Index, rf.currentTerm, true
}


//
// Runs the consensus protocol to replicate the log at followers.
//
// By the time this terminates, the command has either been committed or
// discarded for deletion.
//
func (rf *Raft) getConsensus(newEntry *LogEntry) {
	debugln(ConsensusStream, fmt.Sprintf("Term %v: %v (leader) began consensus for entry #%v", rf.currentTerm, rf.me, newEntry.Index))

	// Mark replicated on myself
	newEntry.NumReplications++

	// Wait for consensus outcome
	outcome := <-newEntry.ConsensusOutcome
	newEntry.IsPendingConsensus = false
	if outcome == Success {
		debugln(ConsensusStream, fmt.Sprintf("Term %v: %v (leader) got consensus for entry #%v", rf.currentTerm, rf.me, newEntry.Index))

		// Consensus was reached! I can commit/apply this entry
		func() {
			rf.mu.Lock()  // need to apply entries atomically
			defer rf.mu.Unlock()

			if rf.lastApplied < newEntry.Index && !(newEntry.Term < rf.currentTerm) {
				rf.applyLogEntries(newEntry.Index)
			}
		}()
	} else if outcome == Stale {
		// Do nothing. If stale, this entry will be deleted soon anyway.
	}
}


//
// Applies all log entries not yet applied up through newCommitIndex
// (inclusive). By the time this returns, all log entries are applied, and
// lastApplied and commitIndex are both be equal to newCommitIndex.
//
// This function should be executed while rf is *locked*.
//
func (rf *Raft) applyLogEntries(newCommitIndex int) {
	oldLastApplied := rf.lastApplied

	debugln(ConsensusStream, fmt.Sprintf("Term %v: %v (leader? %v) is applying entries #%v to #%v", rf.currentTerm, rf.me, (rf.leaderStatus == Leader), oldLastApplied+1, newCommitIndex))

	// Mark up to newCommitIndex as "committed"
	rf.commitIndex = newCommitIndex

	// Apply newly committed entries
	for i := rf.lastApplied+1; i <= rf.commitIndex; i++ {
		entry := rf.log[i]

		applyMsg := ApplyMsg{}
		applyMsg.Index = entry.Index
		applyMsg.Command = entry.Command

		rf.applyCh <- applyMsg
	}
	rf.lastApplied = rf.commitIndex

	rf.logString = rf.logToString()  // DEBUG

	debugln(ConsensusStream, fmt.Sprintf("Term %v: %v (leader? %v) finished applying entries #%v to #%v. (log = %v)", rf.currentTerm, rf.me, (rf.leaderStatus == Leader), oldLastApplied+1, newCommitIndex, rf.logString))
}


//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.

	debugln(ConsensusStream, fmt.Sprintf("%v was shut down at term %v. (lastLogIndex = %v, lastLogTerm = %v, commitIndex = %v, leaderStatus = %v)", rf.me, rf.currentTerm, len(rf.log)-1, rf.lastLogTerm(), rf.commitIndex, rf.leaderStatus))
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.

	// Initialize random number generator (for getting random timer intervals)
	rand.Seed(time.Now().UnixNano())

	// Initialize general attributes of the server
	rf.currentTerm = 0

	// Initialize attributes for elections
	rf.votedFor = NoOne
	rf.leaderStatus = Follower
	rf.heartbeatTimer = nil   // start timer when it's needed
	rf.electionTimer = nil    // start timer when it's needed
	rf.electionOutcome = make(chan int, 1)
	rf.numVotes = 0

	// Initialize attributes for consensus
	rf.log = make([]*LogEntry, 1)  // TODO: Note that there's a dummy value so we can index from 1. Make sure to account for this in the rest of the code.
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.appendEntriesId = 0
	rf.logString = rf.logToString()  // DEBUG

	// Ready for heartbeat messages
	go rf.waitForLeaderToDie()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
