package chandy_lamport

import (
	"log"
	"math/rand"
)

// Max random delay added to packet delivery
const maxDelay = 5

// Simulator is the entry point to the distributed snapshot application.
//
// It is a discrete time simulator, i.e. events that happen at time t + 1 come
// strictly after events that happen at time t. At each time step, the simulator
// examines messages queued up across all the links in the system and decides
// which ones to deliver to the destination.
//
// The simulator is responsible for starting the snapshot process, inducing servers
// to pass tokens to each other, and collecting the snapshot state after the process
// has terminated.
type Simulator struct {
	time           int
	nextSnapshotId int
	servers        map[string]*Server // key = server ID
	logger         *Logger
	// TODO: ADD MORE FIELDS HERE

	// Field for collecting global snapshot
	isDoneSnapshotting  *SyncMap   // serverId -> done snapshotting? (chan bool,
	                               // where chan will hold only 1 value)
}

func NewSimulator() *Simulator {
	return &Simulator{
		0,
		0,
		make(map[string]*Server),
		NewLogger(),
		NewSyncMap(),
	}
}

// Return the receive time of a message after adding a random delay.
// Note: since we only deliver one message to a given server at each time step,
// the message may be received *after* the time step returned in this function.
func (sim *Simulator) GetReceiveTime() int {
	return sim.time + 1 + rand.Intn(5)
}

// Add a server to this simulator with the specified number of starting tokens
func (sim *Simulator) AddServer(id string, tokens int) {
	server := NewServer(id, tokens, sim)
	sim.servers[id] = server
}

// Add a unidirectional link between two servers
func (sim *Simulator) AddForwardLink(src string, dest string) {
	server1, ok1 := sim.servers[src]
	server2, ok2 := sim.servers[dest]
	if !ok1 {
		log.Fatalf("Server %v does not exist\n", src)
	}
	if !ok2 {
		log.Fatalf("Server %v does not exist\n", dest)
	}
	server1.AddOutboundLink(server2)
}

// Run an event in the system
func (sim *Simulator) InjectEvent(event interface{}) {
	switch event := event.(type) {
	case PassTokenEvent:
		src := sim.servers[event.src]
		src.SendTokens(event.tokens, event.dest)
	case SnapshotEvent:
		sim.StartSnapshot(event.serverId)
	default:
		log.Fatal("Error unknown event: ", event)
	}
}

// Advance the simulator time forward by one step, handling all send message events
// that expire at the new time step, if any.
func (sim *Simulator) Tick() {
	sim.time++
	sim.logger.NewEpoch()
	// Note: to ensure deterministic ordering of packet delivery across the servers,
	// we must also iterate through the servers and the links in a deterministic way
	for _, serverId := range getSortedKeys(sim.servers) {
		server := sim.servers[serverId]
		for _, dest := range getSortedKeys(server.outboundLinks) {
			link := server.outboundLinks[dest]
			// Deliver at most one packet per server at each time step to
			// establish total ordering of packet delivery to each server
			if !link.events.Empty() {
				e := link.events.Peek().(SendMessageEvent)
				if e.receiveTime <= sim.time {
					link.events.Pop()
					sim.logger.RecordEvent(
						sim.servers[e.dest],
						ReceivedMessageEvent{e.src, e.dest, e.message})
					sim.servers[e.dest].HandlePacket(e.src, e.message)
					break
				}
			}
		}
	}
}

// Start a new snapshot process at the specified server
func (sim *Simulator) StartSnapshot(serverId string) {
	snapshotId := sim.nextSnapshotId
	sim.nextSnapshotId++
	sim.logger.RecordEvent(sim.servers[serverId], StartSnapshot{serverId, snapshotId})
	// TODO: IMPLEMENT ME

	// Init isDoneSnapshotting map, which maps each server (by id) to a boolean
	// indicating whether it's done snapshotting. Store this boolean in a
	// *channel* to automatically block until a server is done.
	//
	// (See CollectSnapshot() for how this is used)
	for _, serverId := range getSortedKeys(sim.servers) {
		ch := make(chan bool, 1)
		sim.isDoneSnapshotting.Store(serverId, ch)
	}

	// Tell server to start snapshot
	server := sim.servers[serverId]
	server.StartSnapshot(snapshotId)
}

// Callback for servers to notify the simulator that the snapshot process has
// completed on a particular server
func (sim *Simulator) NotifySnapshotComplete(serverId string, snapshotId int) {
	sim.logger.RecordEvent(sim.servers[serverId], EndSnapshot{serverId, snapshotId})
	// TODO: IMPLEMENT ME

	// Mark that this server is done with snapshot
	//
	// We send `true` into this server's chan. CollectSnapshot() reads from this
	// chan to determine when the server is done.
	val, ok := sim.isDoneSnapshotting.Load(serverId)
	checkOk(ok, "Error: sim.isDoneSnapshotting.Load() failed")
	ch := val.(chan bool)
	ch <- true
}

// Collect and merge snapshot state from all the servers.
// This function blocks until the snapshot process has completed on all servers.
func (sim *Simulator) CollectSnapshot(snapshotId int) *SnapshotState {
	// TODO: IMPLEMENT ME

	// Wait until all servers are done
	//
	// Pulling a value off a server's chan indicates that it's done
	// (NotifySnapshotComplete() puts these values on the chan.)
	for _, serverId := range getSortedKeys(sim.servers) {
		val, ok := sim.isDoneSnapshotting.Load(serverId)
		checkOk(ok, "Error: sim.isDoneSnapshotting.Load() failed")
		ch := val.(chan bool)
		if <-ch {
			continue  // this server is done
		}
	}

	// Collect global snapshot from local snapshots of each server
	tokensByServerId := make(map[string]int)
	messagesInTransit := make([]*SnapshotMessage, 0)
	for _, serverId := range getSortedKeys(sim.servers) {
		server := sim.servers[serverId]

		// Collect num tokens in this server's snapshot
		tokensByServerId[serverId] = server.TokensInSnapshot

		// Collect messages received by this server
		for _, msg := range server.MsgsRecvd {
			messagesInTransit = append(messagesInTransit, msg)
		}
	}

	snap := SnapshotState{snapshotId, tokensByServerId, messagesInTransit}
	return &snap
}
