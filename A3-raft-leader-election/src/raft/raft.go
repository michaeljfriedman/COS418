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
import "math/rand"
import "sync"
import "time"

// import "bytes"
// import "encoding/gob"

// Leader statuses
const Leader = 0
const Candidate = 1
const Follower = 2

// Election outcomes (relative to this server)
const Won = 0
const Lost = 1
const Timeout = 2

// Set votedFor to NoOne to indicate no vote granted yet in an
// election
const NoOne = -1

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

	// General attributes
	// Refer to paper section 5 for any uncommented attributes
	currentTerm   int
	votedFor      int  // server id, or NoOne
	log           []*LogEntry
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int

	// Attributes for elections
	leaderStatus    int         // Leader, Candidate, or Follower
	heartbeatTimer  *time.Timer // time until I consider leader dead
	electionTimer   *time.Timer // time until I restart an election
	electionOutcome chan int    // Won, Lost, or Timeout. Use chan so reading will
	                            // block until an election has ended
	numVotes        int
}

//
// A log entry
//
type LogEntry struct {
	command interface{}
	term    int
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
// Helper function to generate a timer with a random time
// interval
//
func randomTimer() *time.Timer {
	minMillisecs := 150
	maxMillisecs := 300

	randMillisecs := minMillisecs + rand.Intn(maxMillisecs - minMillisecs)
	return time.NewTimer(time.Millisecond * time.Duration(randMillisecs))
}

//
// Start a timer for heartbeat messages from the leader. When the timer expires,
// assume the leader is dead, and start an election to run for leader.
//
// Outside this function, the timer should be stopped and this function should
// be called again when a heartbeat is received.
//
func (rf *Raft) WaitForLeaderToDie() {
	rf.heartbeatTimer = randomTimer()

	<-rf.heartbeatTimer.C
	go rf.RunForLeader()
}

//
// example RequestVote RPC arguments structure.
// See paper section 5.2 for reference
//
type RequestVoteArgs struct {
	// Your data here.
	term         int
	candidateId  int
	lastLogIndex int
	lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// See paper section 5.2 for reference
//
type RequestVoteReply struct {
	// Your data here.
	term        int
	voteGranted bool
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
	if args.lastLogTerm > rf.log[rf.commitIndex].term {
		return true
	}

	// If terms are equal, then their log is more up to date if it's longer.
	if args.lastLogTerm == rf.log[rf.commitIndex].term && args.lastLogIndex > rf.commitIndex {
		return true
	}

	// Neither test passed. Candidate's log must not be as up to
	return false
}

//
// Handles a RequestVote message from a candidate. I grant a vote
// for the candidate if they are eligible to run (see canBeLeader()) and
// if I have not yet voted in this term.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	// TODO: Implement RequestVote()
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
// An AppendEntries message (see paper section 5 for reference)
// Acts as a heartbeat message in this assignment
//
type AppendEntries struct {
	term int
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args AppendEntries) {
	// TODO: Implement AppendEntries()
}

//
// Start a new election, and run for leader.
// See paper section 5.2 for reference.
//
func (rf *Raft) RunForLeader() {
	// TODO: Implement RunForLeader()
}


//------------------------------------------------------------------------------

// Logging/executing commands and leader-follower consensus

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
	go rf.DoConsensus(command)
	return rf.commitIndex + 1, rf.currentTerm, true
}


//
// Logs the given command and runs the consensus protocol to replicate
// the log at followers.
//
func (rf *Raft) DoConsensus(command interface{}) {
	// TODO: Implement DoConsensus()
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
	rf.votedFor = NoOne
	rf.log = make([]*LogEntry, 1)  // TODO: Note that there's a dummy value so we can index from 1. Make sure to account for this in the rest of the code.
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// Initialize attributes for elections
	rf.leaderStatus = Follower
	rf.heartbeatTimer = nil   // start timer when it's needed
	rf.electionTimer = nil    // start timer when it's needed
	rf.electionOutcome = make(chan int, 1)
	rf.numVotes = 0

	// Ready for heartbeat messages
	go rf.WaitForLeaderToDie()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
