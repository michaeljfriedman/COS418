package chandy_lamport

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
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

	// Fields for collecting global snapshot
	mu sync.Mutex

	// (server id, snapshot id) pair -> is that server done with that snapshot?
	// (implemented as a channel, which holds only 1 bool value)
	isDone map[ServerSnapshotPair]chan bool
}

// A container for a server id and snapshot id.
type ServerSnapshotPair struct {
	serverId   string
	snapshotId int
}

func NewSimulator() *Simulator {
	return &Simulator{
		0,
		0,
		make(map[string]*Server),
		NewLogger(),
		sync.Mutex{},
		make(map[ServerSnapshotPair]chan bool),
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

	// Init isDone map, which maps each (snapshot id, server id) pair to a boolean
	// indicating whether it's done snapshotting. Store this boolean in a
	// *channel* to automatically block until a server is done.
	//
	// (See CollectSnapshot() for how this is used)
	sim.mu.Lock()
	for serverId, _ := range sim.servers {
		ch := make(chan bool, 1)
		sim.isDone[ServerSnapshotPair{serverId, snapshotId}] = ch
	}
	sim.mu.Unlock()

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
	// Do this by sending `true` into this server's chan. CollectSnapshot() reads
	// from this chan to determine when the server is done.
	sim.mu.Lock()
	ch, ok := sim.isDone[ServerSnapshotPair{serverId, snapshotId}]
	sim.mu.Unlock()

	checkOk(ok, fmt.Sprintf("Error: retrieving isDone chan for snapshot %v, server %v failed", snapshotId, serverId))

	ch <- true // send true outside the locked section because this may block
}

// Collect and merge snapshot state from all the servers.
// This function blocks until the snapshot process has completed on all servers.
func (sim *Simulator) CollectSnapshot(snapshotId int) *SnapshotState {
	// TODO: IMPLEMENT ME

	// Wait until all servers are done
	//
	// Pulling a value off a server's chan indicates that it's done
	// (NotifySnapshotComplete() puts these values on the chan.)
	for serverId, _ := range sim.servers {
		sim.mu.Lock()
		ch, ok := sim.isDone[ServerSnapshotPair{serverId, snapshotId}]
		sim.mu.Unlock()

		checkOk(ok, fmt.Sprintf("Error: retrieving isDone chan for snapshot %v, server %v failed", snapshotId, serverId))
		<-ch // read from chan outside the locked section because this will block
	}

	// Collect global snapshot from local snapshots of each server
	tokensByServerId := make(map[string]int)
	messagesInTransit := make([]*SnapshotMessage, 0)
	for serverId, server := range sim.servers {
		// Get server's local snapshot for this snapshot id
		server.mu.Lock()
		localSnapshot, ok := server.snapshots[snapshotId]
		server.mu.Unlock()
		checkOk(ok, fmt.Sprintf("Error: retrieving snapshot %v from server %v failed", snapshotId, server.Id))

		// Collect num tokens in this server's local snapshot
		tokensByServerId[serverId] = localSnapshot.Tokens

		// Collect messages received by this server
		for _, msg := range localSnapshot.MsgsRecvd {
			messagesInTransit = append(messagesInTransit, msg)
		}
	}

	snap := SnapshotState{snapshotId, tokensByServerId, messagesInTransit}
	return &snap
}
