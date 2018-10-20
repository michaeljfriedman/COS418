package chandy_lamport

import (
	"fmt"
	"log"
	"sync"
)

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE

	// Keeps track of snapshots with LocalSnapshotStates
	mu        sync.Mutex
	snapshots map[int]*LocalSnapshotState // snapshot id -> local snapshot state
}

// A container for this server's local snapshot state
type LocalSnapshotState struct {
	mu                 sync.Mutex
	Tokens             int
	MsgsRecvd          []*SnapshotMessage
	isRecordingLink    map[string]bool // server id -> am I recording tokens from that server?
	linksDoneRecording int
	isDone             bool
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		sync.Mutex{},
		make(map[int]*LocalSnapshotState),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Helper function for checking "ok" boolean values. Crashes if `ok` is false,
// printing error message `msg`.
func checkOk(ok bool, msg string) {
	if !ok {
		log.Fatalln(msg)
	}
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME

	switch message := message.(type) {
	case TokenMessage:
		server.Tokens += message.numTokens

		// Record the message in each snapshot, if appropriate
		server.mu.Lock()
		for _, snapshot := range server.snapshots {
			// Record if this snapshot is in progress
			if !snapshot.isDone {
				snapshot.mu.Lock()
				if snapshot.isRecordingLink[src] {
					snapshot.MsgsRecvd = append(snapshot.MsgsRecvd, &SnapshotMessage{src, server.Id, message})
				}
				snapshot.mu.Unlock()
			}
		}
		server.mu.Unlock()

	case MarkerMessage:
		// Get snapshot corresponding to this marker
		server.mu.Lock()
		snapshotId := message.snapshotId
		snapshot, isSnapshotStarted := server.snapshots[snapshotId]
		server.mu.Unlock()

		// Start snapshotting if I haven't started yet
		if !isSnapshotStarted {
			server.StartSnapshot(snapshotId)
		}

		// Get snapshot again in case it was just started
		server.mu.Lock()
		snapshot, ok := server.snapshots[snapshotId]
		server.mu.Unlock()

		checkOk(ok, fmt.Sprintf("Error: retrieving snapshot %v failed", snapshotId))

		// Determine whether this snapshot is done, and the return value indicates
		// whether it's done. Do this locked for concurrency safe reads/writes.
		done := func() bool {
			snapshot.mu.Lock()
			defer snapshot.mu.Unlock()

			// Stop recording messages from this sender
			snapshot.isRecordingLink[src] = false
			snapshot.linksDoneRecording++

			// Stop snapshotting if I'm done recording all links, and notify
			// simulator.
			if !snapshot.isDone && snapshot.linksDoneRecording == len(server.inboundLinks) {
				snapshot.isDone = true
				return true
			}
			return false
		}()

		if done {
			server.sim.NotifySnapshotComplete(server.Id, snapshotId)
		}
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME

	// Record my state, and start recording state of inbound links
	tokensInSnapshot := server.Tokens
	msgsRecvd := make([]*SnapshotMessage, 0)
	isRecordingLink := make(map[string]bool)
	for serverId, _ := range server.inboundLinks {
		isRecordingLink[serverId] = true
	}
	linksDoneRecording := 0
	isDone := false

	server.mu.Lock()
	server.snapshots[snapshotId] = &LocalSnapshotState{
		sync.Mutex{},
		tokensInSnapshot,
		msgsRecvd,
		isRecordingLink,
		linksDoneRecording,
		isDone,
	}
	server.mu.Unlock()

	// Send markers out to all neighbors
	server.SendToNeighbors(MarkerMessage{snapshotId})
}
