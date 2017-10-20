package chandy_lamport

import "sync"
import "log"

// Concurrency-safe boolean value. Synchronizes reads/writes
type SyncBool struct {
	b bool
	lock sync.RWMutex
}

// Returns new SyncBool
func NewSyncBool() *SyncBool {
	syncBool := SyncBool{}
	return &syncBool
}

// Sets value of this bool
func (syncBool *SyncBool) Set(val bool) {
	syncBool.lock.Lock()
	defer syncBool.lock.Unlock()
	syncBool.b = val
}

// Gets value of this bool
func (syncBool *SyncBool) Get() bool {
	syncBool.lock.Lock()
	defer syncBool.lock.Unlock()
	return syncBool.b
}

//--------------------------------------

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id                  string
	Tokens              int
	sim                 *Simulator
	outboundLinks       map[string]*Link // key = link.dest
	inboundLinks        map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE
	TokensInSnapshot    int
	TokensInLink        *SyncMap  // link.src (inbound) -> # tokens
	isRecordingLink     *SyncMap  // link.src (inbound) -> is recording tokens?
	isSnapshotting      *SyncBool
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

	switch msg := msg.(type) {
		case TokenMessage:
			server.Tokens++

			// Increment num tokens in this inbound link, if appropriate
			isRecording, ok := server.isRecordingLink.Load(src)
			checkOk(ok)
			if server.isSnapshotting.Get() && isRecording {
				tokensInLink, ok := server.TokensInLink.Load(src)
				checkOk(ok, "Error: server.TokensInLink.Load() failed")
				server.TokensInLink.Store(src, tokensInLink + 1)
			}
		case MarkerMessage:
			snapshotId := msg.snapshotId

			// Start snapshotting if I haven't started yet
			if !server.isSnapshotting.Get() {
				server.StartSnapshot(snapshotId)
			}

			// Stop recording messages from this sender
			server.isRecordingLink.Store(src, false)

			// Stop snapshotting if this was the last marker received, and notify
			// simulator.
			doneRecording := true
			for _, serverId := range getSortedKeys(server.inboundLinks) {
				isRecording, ok := server.isRecordingLink.Load(serverId)
				checkOk(ok, "Error: server.isRecordingLink.Load() failed")
				if isRecording {
					doneRecording = false
					break
				}
			}

			if server.isSnapshotting.Get() && doneRecording {
				server.isSnapshotting.Set(false)
				server.sim.NotifySnapshotComplete(server.Id, snapshotId)
			}
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME

	// Record my state, and start recording state of inbound links
	server.TokensInSnapshot = server.Tokens  // TODO: Note that server.Tokens may need to be locked before reading it. Check here if you encounter bugs.
	server.TokensInLink = NewSyncMap()
	server.isRecordingLink = NewSyncMap()
	for _, serverId := range getSortedKeys(server.inboundLinks) {
		server.TokensInLink.Store(serverId, 0)
		server.isRecordingLink.Store(serverId, true)
	}
	server.isSnapshotting = NewSyncBool()
	server.isSnapshotting.Set(true)

	// Send markers out to all neighbors
	server.SendToNeighbors(MarkerMessage{snapshotId})
}
