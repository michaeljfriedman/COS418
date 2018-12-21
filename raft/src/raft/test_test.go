package raft

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dbg"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const RaftElectionTimeout = 1000 * time.Millisecond

const (
	tagTestFailAgree         = "TestFailAgree"
	tagTestBackup            = "TestBackup"
	tagTestFigure8           = "TestFigure8"
	tagTestUnreliableAgree   = "TestUnreliableAgree"
	tagTestFigure8Unreliable = "TestFigure8Unreliable"
	tagTestReliableChurn     = "TestReliableChurn"
	tagTestUnreliableChurn   = "TestUnreliableChurn"
)

func TestInitialElection(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: initial election...")

	// is a leader elected?
	cfg.checkOneLeader()

	// does the leader+term stay the same there is no failure?
	term1 := cfg.checkTerms()
	time.Sleep(2 * RaftElectionTimeout)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		fmt.Println("warning: term changed even though there were no failures")
	}

	fmt.Println("... Passed")
}

func TestReElection(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: election after network failure...")

	leader1 := cfg.checkOneLeader()

	// if the leader disconnects, a new one should be elected.
	cfg.disconnect(leader1)
	cfg.checkOneLeader()

	// if the old leader rejoins, that shouldn't
	// disturb the old leader.
	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()

	// if there's no quorum, no leader should
	// be elected.
	cfg.disconnect(leader2)
	cfg.disconnect((leader2 + 1) % servers)
	time.Sleep(2 * RaftElectionTimeout)
	cfg.checkNoLeader()

	// if a quorum arises, it should elect a leader.
	cfg.connect((leader2 + 1) % servers)
	cfg.checkOneLeader()

	// re-join of last node shouldn't prevent leader from existing.
	cfg.connect(leader2)
	cfg.checkOneLeader()

	fmt.Println("... Passed")
}

func TestBasicAgree(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: basic agreement...")

	iters := 3
	for index := 1; index < iters+1; index++ {
		nd, _ := cfg.nCommitted(index)
		if nd > 0 {
			t.Fatal("some have committed before Start()")
		}

		xindex := cfg.one(index*100, servers)
		if xindex != index {
			t.Fatalf("got index %v but expected %v\n", xindex, index)
		}
	}

	fmt.Println("... Passed")
}

func TestFailAgree(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: agreement despite follower failure...")

	dbg.Log("Submitting 1 command", []string{tagTestFailAgree})
	cfg.one(101, servers)

	// follower network failure
	leader := cfg.checkOneLeader()
	follower1 := (leader + 1) % servers
	dbg.Logf("Disconnecting follower %v", []string{tagTestFailAgree}, follower1)
	cfg.disconnect(follower1)

	// agree despite one failed server?
	dbg.Log("Submitting 4 commands", []string{tagTestFailAgree})
	cfg.one(102, servers-1)
	cfg.one(103, servers-1)
	time.Sleep(RaftElectionTimeout)
	cfg.one(104, servers-1)
	cfg.one(105, servers-1)

	// failed server re-connected
	dbg.Logf("Reconnecting follower %v", []string{tagTestFailAgree}, follower1)
	cfg.connect(follower1)

	// agree with full set of servers?
	dbg.Log("Submitting 2 commands", []string{tagTestFailAgree})
	cfg.one(106, servers)
	time.Sleep(RaftElectionTimeout)
	cfg.one(107, servers)

	fmt.Println("... Passed")
}

func TestFailNoAgree(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: no agreement if too many followers fail...")

	cfg.one(10, servers)

	// 3 of 5 followers disconnect
	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % servers)
	cfg.disconnect((leader + 2) % servers)
	cfg.disconnect((leader + 3) % servers)

	index, _, ok := cfg.rafts[leader].Start(20)
	if ok != true {
		t.Fatal("leader rejected Start()")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %v\n", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	n, _ := cfg.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority\n", n)
	}

	// repair failures
	cfg.connect((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.connect((leader + 3) % servers)

	// the disconnected majority may have chosen a leader from
	// among their own ranks, forgetting index 2.
	// or perhaps
	leader2 := cfg.checkOneLeader()
	index2, _, ok2 := cfg.rafts[leader2].Start(30)
	if ok2 == false {
		t.Fatal("leader2 rejected Start()")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %v\n", index2)
	}

	cfg.one(1000, servers)

	fmt.Println("... Passed")
}

func TestConcurrentStarts(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: concurrent Start()s...")

	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader := cfg.checkOneLeader()
		_, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			// leader moved on really quickly
			continue
		}

		iters := 5
		var wg sync.WaitGroup
		is := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				i, term1, ok := cfg.rafts[leader].Start(100 + i)
				if term1 != term {
					return
				}
				if ok != true {
					return
				}
				is <- i
			}(ii)
		}

		wg.Wait()
		close(is)

		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				continue loop
			}
		}

		failed := false
		cmds := []int{}
		for index := range is {
			cmd := cfg.wait(index, servers, term)
			if ix, ok := cmd.(int); ok {
				if ix == -1 {
					// peers have moved on to later terms
					// so we can't expect all Start()s to
					// have succeeded
					failed = true
					break
				}
				cmds = append(cmds, ix)
			} else {
				t.Fatalf("value %v is not an int", cmd)
			}
		}

		if failed {
			// avoid leaking goroutines
			go func() {
				for range is {
				}
			}()
			continue
		}

		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			ok := false
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true
				}
			}
			if ok == false {
				t.Fatalf("cmd %v missing in %v", x, cmds)
			}
		}

		success = true
		break
	}

	if !success {
		t.Fatal("term changed too often")
	}

	fmt.Println("... Passed")
}

func TestRejoin(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: rejoin of partitioned leader...")

	cfg.one(101, servers)

	// leader network failure
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)

	// make old leader try to agree on some entries
	cfg.rafts[leader1].Start(102)
	cfg.rafts[leader1].Start(103)
	cfg.rafts[leader1].Start(104)

	// new leader commits, also for index=2
	cfg.one(103, 2)

	// new leader network failure
	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)

	// old leader connected again
	cfg.connect(leader1)

	cfg.one(104, 2)

	// all together now
	cfg.connect(leader2)

	cfg.one(105, servers)

	fmt.Println("... Passed")
}

func TestBackup(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Print("Test: leader backs up quickly over incorrect follower logs...")

	dbg.Log("Submitting 1 command (101)", []string{tagTestBackup})
	cfg.one(101, servers)

	// put leader and one follower in a partition
	leader1 := cfg.checkOneLeader()
	follower2 := (leader1 + 2) % servers
	follower3 := (leader1 + 3) % servers
	follower4 := (leader1 + 4) % servers
	dbg.Logf("Disconnecting partition 2 (followers %v, %v, %v)", []string{tagTestBackup}, follower2, follower3, follower4)
	cfg.disconnect(follower2)
	cfg.disconnect(follower3)
	cfg.disconnect(follower4)

	// submit lots of commands that won't commit
	dbg.Log("Submitting 50 commands (201-250)", []string{tagTestBackup})
	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Start(201 + i)
	}

	time.Sleep(RaftElectionTimeout / 2)

	follower1 := (leader1 + 1) % servers
	dbg.Logf("Reconnecting partition 2 (followers %v, %v, %v), and disconnecting partition 1 (leader %v and follower %v)", []string{tagTestBackup}, follower2, follower3, follower4, leader1, follower1)
	cfg.disconnect(leader1)
	cfg.disconnect(follower1)

	// allow other partition to recover
	cfg.connect(follower2)
	cfg.connect(follower3)
	cfg.connect(follower4)

	// lots of successful commands to new group.
	dbg.Log("Submitting 50 commands (301-350)", []string{tagTestBackup})
	for i := 0; i < 50; i++ {
		cfg.one(301+i, 3)
	}

	// now another partitioned leader and one follower
	leader2 := cfg.checkOneLeader()
	other := follower2
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	dbg.Logf("Disconnecting 'other' in partition 2 (follower %v)", []string{tagTestBackup}, other)
	cfg.disconnect(other)

	// lots more commands that won't commit
	dbg.Log("Submitting 50 commands (401-450)", []string{tagTestBackup})
	for i := 0; i < 50; i++ {
		cfg.rafts[leader2].Start(401 + i)
	}

	time.Sleep(RaftElectionTimeout / 2)

	// bring original leader back to life,
	dbg.Logf("Reconnecting partition 1 (leader %v and follower %v) and 'other' %v", []string{tagTestBackup}, leader1, follower1, other)
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}
	cfg.connect(leader1)
	cfg.connect(follower1)
	cfg.connect(other)

	// lots of successful commands to new group.
	dbg.Logf("Submitting 50 commands (501-550)", []string{tagTestBackup})
	for i := 0; i < 50; i++ {
		cfg.one(501+i, 3)
	}

	// now everyone
	dbg.Logf("Reconnecting everyone", []string{tagTestBackup})
	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}
	dbg.Logf("Submitting 1 command (601)", []string{tagTestBackup})
	cfg.one(601, servers)

	fmt.Println("... Passed")
}

func TestCount(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: RPC counts aren't too high...")

	rpcs := func() (n int) {
		for j := 0; j < servers; j++ {
			n += cfg.rpcCount(j)
		}
		return
	}

	leader := cfg.checkOneLeader()

	total1 := rpcs()

	if total1 > 30 || total1 < 1 {
		t.Fatalf("too many or few RPCs (%v) to elect initial leader\n", total1)
	}

	var total2 int
	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader = cfg.checkOneLeader()
		total1 = rpcs()

		iters := 10
		starti, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			// leader moved on really quickly
			continue
		}
		cmds := []int{}
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			index1, term1, ok := cfg.rafts[leader].Start(x)
			if term1 != term {
				// Term changed while starting
				continue loop
			}
			if !ok {
				// No longer the leader, so term has changed
				continue loop
			}
			if starti+i != index1 {
				t.Fatal("Start() failed")
			}
		}

		for i := 1; i < iters+1; i++ {
			cmd := cfg.wait(starti+i, servers, term)
			if ix, ok := cmd.(int); ok == false || ix != cmds[i-1] {
				if ix == -1 {
					// term changed -- try again
					continue loop
				}
				t.Fatalf("wrong value %v committed for index %v; expected %v\n", cmd, starti+i, cmds)
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				// need to keep going to update total2
				failed = true
			}
			total2 += cfg.rpcCount(j)
		}

		if failed {
			continue loop
		}

		if total2-total1 > (iters+1+3)*3 {
			t.Fatalf("too many RPCs (%v) for %v entries\n", total2-total1, iters)
		}

		success = true
		break
	}

	if !success {
		t.Fatal("term changed too often")
	}

	time.Sleep(RaftElectionTimeout)

	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += cfg.rpcCount(j)
	}

	if total3-total2 > 3*20 {
		t.Fatalf("too many RPCs (%v) for 1 second of idleness\n", total3-total2)
	}

	fmt.Println("... Passed")
}

func TestPersist1(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: basic persistence...")

	cfg.one(11, servers)

	// crash and re-start all
	for i := 0; i < servers; i++ {
		cfg.start1(i)
	}
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
		cfg.connect(i)
	}

	cfg.one(12, servers)

	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)
	cfg.start1(leader1)
	cfg.connect(leader1)

	cfg.one(13, servers)

	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)
	cfg.one(14, servers-1)
	cfg.start1(leader2)
	cfg.connect(leader2)

	cfg.wait(4, servers, -1) // wait for leader2 to join before killing i3

	i3 := (cfg.checkOneLeader() + 1) % servers
	cfg.disconnect(i3)
	cfg.one(15, servers-1)
	cfg.start1(i3)
	cfg.connect(i3)

	cfg.one(16, servers)

	fmt.Println("... Passed")
}

func TestPersist2(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: more persistence...")

	index := 1
	for iters := 0; iters < 5; iters++ {
		cfg.one(10+index, servers)
		index++

		leader1 := cfg.checkOneLeader()

		cfg.disconnect((leader1 + 1) % servers)
		cfg.disconnect((leader1 + 2) % servers)

		cfg.one(10+index, servers-2)
		index++

		cfg.disconnect((leader1 + 0) % servers)
		cfg.disconnect((leader1 + 3) % servers)
		cfg.disconnect((leader1 + 4) % servers)

		cfg.start1((leader1 + 1) % servers)
		cfg.start1((leader1 + 2) % servers)
		cfg.connect((leader1 + 1) % servers)
		cfg.connect((leader1 + 2) % servers)

		time.Sleep(RaftElectionTimeout)

		cfg.start1((leader1 + 3) % servers)
		cfg.connect((leader1 + 3) % servers)

		cfg.one(10+index, servers-2)
		index++

		cfg.connect((leader1 + 4) % servers)
		cfg.connect((leader1 + 0) % servers)
	}

	cfg.one(1000, servers)

	fmt.Println("... Passed")
}

func TestPersist3(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: partitioned leader and one follower crash, leader restarts...")

	cfg.one(101, 3)

	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 2) % servers)

	cfg.one(102, 2)

	cfg.crash1((leader + 0) % servers)
	cfg.crash1((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.start1((leader + 0) % servers)
	cfg.connect((leader + 0) % servers)

	cfg.one(103, 2)

	cfg.start1((leader + 1) % servers)
	cfg.connect((leader + 1) % servers)

	cfg.one(104, servers)

	fmt.Println("... Passed")
}

//
// Test the scenarios described in Figure 8 of the extended Raft paper. Each
// iteration asks a leader, if there is one, to insert a command in the Raft
// log.  If there is a leader, that leader will fail quickly with a high
// probability (perhaps without committing the command), or crash after a while
// with low probability (most likey committing the command).  If the number of
// alive servers isn't enough to form a majority, perhaps start a new server.
// The leader in a new term may try to finish replicating log entries that
// haven't been committed yet.
//
func TestFigure8(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Println("Test: Figure 8...")

	cmd := 1
	dbg.Logf("Submitting command %v, waiting for replication on 1 server", []string{tagTestFigure8}, cmd)
	cfg.one(cmd, 1)

	nup := servers
	niters := 1000
	for iters := 0; iters < niters; iters++ {
		cmd++

		dbg.Logf("Submitting command %v to all servers, looking for leader", []string{tagTestFigure8}, cmd)
		leader := -1
		for i := 0; i < servers; i++ {
			if cfg.rafts[i] != nil {
				_, _, ok := cfg.rafts[i].Start(cmd)
				if ok {
					leader = i
				}
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 {
			dbg.Logf("Crashing leader %v", []string{tagTestFigure8}, leader)
			cfg.crash1(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.rafts[s] == nil {
				dbg.Logf("Restarting and reconnecting server %v", []string{tagTestFigure8}, s)
				cfg.start1(s)
				cfg.connect(s)
				nup += 1
			}
		}
	}

	dbg.Logf("Restarting and reconnecting all (%v) servers", []string{tagTestFigure8}, servers)
	for i := 0; i < servers; i++ {
		if cfg.rafts[i] == nil {
			cfg.start1(i)
			cfg.connect(i)
		}
	}

	cmd++
	dbg.Logf("Submitting command %v, waiting for replication on all (%v) servers", []string{tagTestFigure8}, cmd, servers)
	cfg.one(cmd, servers)

	fmt.Println("... Passed")
}

func TestUnreliableAgree(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true)
	defer cfg.cleanup()

	fmt.Println("Test: unreliable agreement...")

	var wg sync.WaitGroup

	for iters := 1; iters < 50; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				cmd := (100 * iters) + j
				dbg.Logf("Submitting command %v, waiting for replication on 1 server", []string{tagTestUnreliableAgree}, cmd)
				cfg.one(cmd, 1)
			}(iters, j)
		}
		dbg.Logf("Submitting command %v, waiting for replication on 1 server", []string{tagTestUnreliableAgree}, iters)
		cfg.one(iters, 1)
	}

	dbg.Log("Enabling unreliable mode", []string{tagTestUnreliableAgree})
	cfg.setunreliable(false)

	wg.Wait()

	cmd := 100
	dbg.Logf("Submitting command %v, waiting for replication on all (%v) servers", []string{tagTestUnreliableAgree}, cmd, servers)
	cfg.one(cmd, servers)

	fmt.Println("... Passed")
}

func TestFigure8Unreliable(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true)
	defer cfg.cleanup()

	fmt.Println("Test: Figure 8 (unreliable)...")

	cmd := 1
	dbg.Logf("Submitting command %v, waiting for replication on only 1 server", []string{tagTestFigure8Unreliable}, cmd)
	cfg.one(cmd, 1)

	nup := servers
	niters := 1000
	for iters := 0; iters < niters; iters++ {
		cmd++

		if iters == 200 {
			dbg.Log("Enabling long reordering", []string{tagTestFigure8Unreliable})
			cfg.setlongreordering(true)
		}
		dbg.Logf("Submitting command %v to all servers, looking for leader", []string{tagTestFigure8Unreliable}, cmd)
		leader := -1
		for i := 0; i < servers; i++ {
			_, _, ok := cfg.rafts[i].Start(cmd)
			if ok && cfg.connected[i] {
				leader = i
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && (rand.Int()%1000) < int(RaftElectionTimeout/time.Millisecond)/2 {
			dbg.Logf("Disconnecting leader %v", []string{tagTestFigure8Unreliable}, leader)
			cfg.disconnect(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.connected[s] == false {
				dbg.Logf("Reconnecting server %v", []string{tagTestFigure8Unreliable}, s)
				cfg.connect(s)
				nup += 1
			}
		}
	}

	dbg.Log("Reconnecting all servers", []string{tagTestFigure8Unreliable})
	for i := 0; i < servers; i++ {
		if cfg.connected[i] == false {
			cfg.connect(i)
		}
	}

	cmd++
	dbg.Logf("Submitting command %v, waiting for replication on all (%v) servers", []string{tagTestFigure8Unreliable}, cmd, servers)
	cfg.one(cmd, servers)

	fmt.Println("... Passed")
}

func internalChurn(t *testing.T, unreliable bool, tag string) {

	if unreliable {
		fmt.Println("Test: unreliable churn...")
	} else {
		fmt.Println("Test: churn...")
	}

	servers := 5
	cfg := make_config(t, servers, unreliable)
	defer cfg.cleanup()

	stop := int32(0)

	// create concurrent clients
	cfn := func(me int, ch chan []int) {
		var ret []int
		ret = nil
		defer func() { ch <- ret }()
		values := []int{}
		iter := 1
		for atomic.LoadInt32(&stop) == 0 {
			x := (me * 1000) + iter
			iter++

			dbg.Logf("Submitting command %v on all servers, looking for leader", []string{tag}, x)
			index := -1
			ok := false
			for i := 0; i < servers; i++ {
				// try them all, maybe one of them is a leader
				cfg.mu.Lock()
				rf := cfg.rafts[i]
				cfg.mu.Unlock()
				if rf != nil {
					index1, _, ok1 := rf.Start(x)
					if ok1 {
						ok = ok1
						index = index1
					}
				}
			}
			if ok {
				// maybe leader will commit our value, maybe not.
				// but don't wait forever.
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, cmd := cfg.nCommitted(index)
					if nd > 0 {
						if xx, ok := cmd.(int); ok {
							if xx == x {
								values = append(values, x)
							}
						} else {
							cfg.t.Fatal("wrong command type")
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
		ret = values
	}

	ncli := 3
	cha := []chan []int{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []int))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			dbg.Logf("Disconnecting server %v", []string{tag}, i)
			cfg.disconnect(i)
		}

		if (rand.Int() % 1000) < 500 {
			i := rand.Int() % servers
			if cfg.rafts[i] == nil {
				dbg.Logf("Restarting server %v", []string{tag}, i)
				cfg.start1(i)
			}
			dbg.Logf("Reconnecting server %v", []string{tag}, i)
			cfg.connect(i)
		}

		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			if cfg.rafts[i] != nil {
				dbg.Logf("Crashing server %v", []string{tag}, i)
				cfg.crash1(i)
			}
		}

		// Make crash/restart infrequent enough that the peers can often
		// keep up, but not so infrequent that everything has settled
		// down from one change to the next. Pick a value smaller than
		// the election timeout, but not hugely smaller.
		time.Sleep((RaftElectionTimeout * 7) / 10)
	}

	time.Sleep(RaftElectionTimeout)
	dbg.Log("Enabling unreliable mode", []string{tag})
	cfg.setunreliable(false)
	dbg.Logf("Starting and connecting all (%v) servers", []string{tag}, servers)
	for i := 0; i < servers; i++ {
		if cfg.rafts[i] == nil {
			cfg.start1(i)
		}
		cfg.connect(i)
	}

	atomic.StoreInt32(&stop, 1)

	values := []int{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatal("client failed")
		}
		values = append(values, vv...)
	}

	time.Sleep(RaftElectionTimeout)

	cmd := 100000
	dbg.Logf("Submitting command %v, waiting for replication on all (%v) servers", []string{tag}, cmd, servers)
	lastIndex := cfg.one(cmd, servers)

	really := make([]int, lastIndex+1)
	for index := 1; index <= lastIndex; index++ {
		v := cfg.wait(index, servers, -1)
		if vi, ok := v.(int); ok {
			really = append(really, vi)
		} else {
			t.Fatal("not an int")
		}
	}

	for _, v1 := range values {
		ok := false
		for _, v2 := range really {
			if v1 == v2 {
				ok = true
			}
		}
		if ok == false {
			cfg.t.Fatal("didn't find a value")
		}
	}

	fmt.Println("... Passed")
}

func TestReliableChurn(t *testing.T) {
	internalChurn(t, false, tagTestReliableChurn)
}

func TestUnreliableChurn(t *testing.T) {
	internalChurn(t, true, tagTestUnreliableChurn)
}
