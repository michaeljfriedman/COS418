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

import "labrpc"
import "log"
import "math/rand"
import "sync"
import "time"

// import "bytes"
// import "encoding/gob"

// DEBUG
// Set this debug flag to true to print out useful log statements during
// leader election.
const debugElection = false

// Set this debug flag to true for log statements during consensus
const debugConsensus = true

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
	PendingConsensus bool     // is this entry pending consensus?
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
// Helper function. Generates and returns a timer with a random time
// interval
//
func randomTimer() *time.Timer {
	minMillisecs := 150
	maxMillisecs := 300

	randMillisecs := minMillisecs + rand.Intn(maxMillisecs - minMillisecs)
	return time.NewTimer(time.Millisecond * time.Duration(randMillisecs))
}

//
// Returns the term of the last committed log entry, or 0 if there are
// no log entries.
//
func (rf *Raft) lastLogTerm() int {
	if rf.commitIndex == 0 {
		return 0
	}
	return rf.log[rf.commitIndex].Term
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
	rf.heartbeatTimer = randomTimer()

	<-rf.heartbeatTimer.C

	// DEBUG
	if debugElection {
		log.Printf("Term %d: %d wants to run. (votedFor = %d)\n", rf.currentTerm, rf.me, rf.votedFor)
	}

	if rf.votedFor == NoOne {
		go rf.runForLeader()
	}
}

//
// As leader, sends heartbeats periodically to other servers to let them
// know I'm still alive.
//
func (rf *Raft) sendPeriodicHeartbeats() {
	intervalMillisecs := 30
	for rf.leaderStatus == Leader {
		for peerId, _ := range rf.peers {
			// Skip me
			if peerId == rf.me {
				continue
			}

			go rf.sendHeartbeatTo(peerId)
		}

		timer := time.NewTimer(time.Millisecond * time.Duration(intervalMillisecs))
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
	if args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.commitIndex {
		return true
	}

	// Neither test passed. Candidate's log must not be as up to
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
	if rf.leaderStatus == Candidate && args.Term > rf.currentTerm {
		// I'm running in an outdated election. I stop my outdated candidacy and
		// vote for you (the caller) in the new election.
		rf.electionOutcome <- Stale
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else if rf.canBeLeader(args) && args.Term > rf.currentTerm && rf.votedFor == NoOne {
		// You (the caller) are eligible to run and I haven't voted yet in this
		// election, so I vote for you.
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
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
		rf.commitIndex,   // last log index
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

	// DEBUG
	if debugElection {
		log.Printf("Term %d: %d received vote reply from %d\n", rf.currentTerm, rf.me, server)
	}

	// Process reply. If applicable, add a vote for me and check if I won
	if rf.leaderStatus == Candidate && reply.Term == rf.currentTerm && reply.VoteGranted {
		// DEBUG
		if debugElection {
			log.Printf("Term %d: %d was voted for by %d\n", rf.currentTerm, rf.me, server)
		}

		rf.numVotes++
		if rf.numVotes == (len(rf.peers) / 2) + 1 {  // majority vote
			rf.electionOutcome <- Won
		}
	} else { // DEBUG
		if debugElection {
			log.Printf("Term %d: %d did not use vote reply from or was denied by %d\n", rf.currentTerm, rf.me, server)
		}
	}
}



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
	Success bool
}

//
// AppendEntries RPC handler. Processes a heartbeat. If I'm the leader,
// I step down to the caller (new leader). If I'm a candidate, I declare
// that I've lost the election to the caller (new leader). If I'm a follower,
// just note the heartbeat to mean the leader is still alive.
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term >= rf.currentTerm {
		// Update my term
		rf.currentTerm = args.Term

		if rf.leaderStatus == Leader {
			// I step down to new leader
			rf.leaderStatus = Follower
			rf.votedFor = NoOne
		} else if rf.leaderStatus == Candidate {
			// I lost
			rf.electionOutcome <- Lost
		} else if rf.leaderStatus == Follower {
			// Reset heartbeat timer for leader
			rf.heartbeatTimer.Stop()

			// Since this may also be the result of an election, reset who I voted for
			rf.votedFor = NoOne
		}

		go rf.waitForLeaderToDie()
	}
}

//
// Calls the AppendEntries RPC on the provided server with args as
// contents. Returns true if the RPC was delivered, or false if it wasn't.
//
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, nil)
	return ok
}

//
// Sends a heartbeat message to the specified server. Should only be called
// by the leader.
//
func (rf *Raft) sendHeartbeatTo(server int) {
	args := AppendEntriesArgs{rf.currentTerm, 0, 0, 0, 0, nil}
	rf.sendAppendEntries(server, args)
}



//
// Start a new election, and run for leader.
// See paper section 5.2 for reference.
//
func (rf *Raft) runForLeader() {
	// Special case - if I'm the only server, I just become leader
	if len(rf.peers) == 1 {
		rf.currentTerm += 1
		rf.leaderStatus = Leader
		return
	}

	//-------------------------------------

	// Initiate the election from my perspective
	rf.currentTerm += 1
	rf.leaderStatus = Candidate
	rf.numVotes = 0

	// Vote for myself
	rf.numVotes++
	rf.votedFor = rf.me

	// DEBUG
	if debugElection {
		log.Printf("Term %d: %d voted for himself\n", rf.currentTerm, rf.me)
	}

	// Start a timer for the election. If the timer expires, the election "timed
	// out" (i.e. no winner)
	rf.electionTimer = randomTimer()
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

	// DEBUG
	if debugElection {
		log.Printf("Term %d: %d requested votes from other servers\n", rf.currentTerm, rf.me)
	}

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
		for peerId, _ := range rf.peers {
			rf.nextIndex[peerId] = len(rf.log) // last log index + 1
			rf.matchIndex[peerId] = 0
		}

		// Start sending periodic heartbeats to other servers to indicate
		// that I'm their new leader
		go rf.sendPeriodicHeartbeats()

		// DEBUG
		if debugConsensus {
			log.Printf("Term %v: %v is now leader\n", rf.currentTerm, rf.me)
		}

		// DEBUG
		if debugElection {
			log.Printf("Term %v: %v has won election and sent notifs to other servers\n", rf.currentTerm, rf.me)
		}
	} else if outcome == Lost {
		// I become follower
		rf.electionTimer.Stop()
		rf.leaderStatus = Follower

		// Reset who I voted for
		rf.votedFor = NoOne
	} else if outcome == Timeout {
		// Restart election
		go rf.runForLeader()
	} else if outcome == Stale {
		// I realized I am running in an outdated election. Stop running.
		rf.electionTimer.Stop()
		rf.leaderStatus = Follower
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
	// Do not accept commands if not the leader
	if rf.leaderStatus != Leader {
		return -1, -1, false
	}

	// Start a consensus procedure on the command
	go rf.doConsensus(command)
	return len(rf.log), rf.currentTerm, true
}


//
// Logs the given command and runs the consensus protocol to replicate
// the log at followers.
//
// By the time this terminates, the command has either been committed or
// discarded or deletion.
//
func (rf *Raft) doConsensus(command interface{}) {
	newEntryIndex := len(rf.log)

	// DEBUG
	if debugConsensus {
		log.Printf("Term %v: %v (leader) began consensus for entry #%v\n", rf.currentTerm, rf.me, newEntryIndex)
	}

	// Create new LogEntry and append it
	newEntry := &LogEntry{
		command,
		rf.currentTerm,
		newEntryIndex,
		0,                 // numReplications
		true,              // pendingConsensus
		make(chan int, 1), // consensusOutcome
	}
	rf.log = append(rf.log, newEntry)

	// DEBUG
	if debugConsensus {
		log.Printf("Term %v: %v (leader) added entry #%v to his log\n", rf.currentTerm, rf.me, newEntryIndex)
	}

	// Wait for consensus outcome
	outcome := <-newEntry.ConsensusOutcome
	if outcome == Success {
		// Consensus was reached! I can commit/apply this entry
		newEntry.PendingConsensus = false

		// DEBUG
		if debugConsensus {
			log.Printf("Term %v: %v (leader) got consensus for entry #%v\n", rf.currentTerm, rf.me, newEntryIndex)
		}

		rf.mu.Lock() // ensure I don't read lastApplied *while* entries are being applied concurrently
		if rf.lastApplied < newEntry.Index && !(newEntry.Term < rf.currentTerm) {
			rf.mu.Unlock()
			rf.applyLogEntries(newEntry.Index)
		} else {
			rf.mu.Unlock()
		}
	} else if outcome == Stale {
		// Do nothing. If stale, this entry will be deleted soon anyway.
	}
}


//
// Applies all log entries not yet applied up through newCommitIndex
// (inclusive). By the time this returns, all log entries are applied, and
// lastApplied and commitIndex are both be equal to newCommitIndex.
//
// This function is atomic and concurrency-safe (i.e. two calls of it cannot be
// running at the same time).
//
func (rf *Raft) applyLogEntries(newCommitIndex int) {
	// DEBUG
	oldLastApplied := rf.lastApplied
	if debugConsensus {
		log.Printf("Term %v: %v (leader) is applying entries #%v to #%v\n", rf.currentTerm, rf.me, oldLastApplied, newCommitIndex)
	}

	// Lock so that lastApplied and commitIndex can't be changed/read
	// while applying entries
	rf.mu.Lock()
	defer rf.mu.Unlock()

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

	// DEBUG
	if debugConsensus {
		log.Printf("Term %v: %v (leader) finished applying entries #%v to #%v\n", rf.currentTerm, rf.me, oldLastApplied, newCommitIndex)
	}
}


//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Ready for heartbeat messages
	go rf.waitForLeaderToDie()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
