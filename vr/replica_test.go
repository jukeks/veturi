package vr

import (
	"bytes"
	"cmp"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"os"
	"slices"
	"testing"
)

type inFlight struct {
	deliverAt int
	out       Outgoing
}

type Sim struct {
	t        *testing.T
	seed     uint64
	rand     *rand.Rand
	now      int
	replicas []*Replica
	down     map[ReplicaNumber]bool
	network  []inFlight

	members  []ReplicaNumber
	executed map[ReplicaNumber][]Payload
	replies  []*ClientReply

	faults Faults

	// invariant state
	primaryOfView map[ViewNumber]ReplicaNumber
	lastCommit    map[ReplicaNumber]CommitNumber
	committed     []Payload             // the longest executed sequence seen
	checkedExec   map[ReplicaNumber]int // executed entries already checked
	seenOps       map[ReplicaNumber]map[string]bool
}

type Faults struct {
	MaxDelay int     // ticks; 0 = deliver in the next tick
	DropRate float64 // 0..1
	DupRate  float64 // 0..1
}

func (f *Faults) delay(r *rand.Rand) int {
	if f.MaxDelay == 0 {
		return 0
	}
	return r.IntN(f.MaxDelay + 1)
}

func (f *Faults) shouldDrop(r *rand.Rand) bool {
	return r.Float64() < f.DropRate
}

func (f *Faults) shouldDup(r *rand.Rand) bool {
	return r.Float64() < f.DupRate
}

func NewSim(t *testing.T, memberCount int, seed uint64) *Sim {
	t.Helper()

	s := &Sim{
		t:        t,
		seed:     seed,
		rand:     rand.New(rand.NewPCG(seed, 0)),
		down:     map[ReplicaNumber]bool{},
		executed: map[ReplicaNumber][]Payload{},

		primaryOfView: map[ViewNumber]ReplicaNumber{},
		lastCommit:    map[ReplicaNumber]CommitNumber{},
		checkedExec:   map[ReplicaNumber]int{},
		seenOps:       map[ReplicaNumber]map[string]bool{},
	}

	for i := range memberCount {
		s.members = append(s.members, ReplicaNumber(i))
	}

	for _, id := range s.members {
		s.replicas = append(s.replicas, NewReplica(id, s.members, s.upCallFor(id), seed+1, false))
	}

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("sim seed %d, failed at tick %d", s.seed, s.now)
		}
	})

	return s
}

// upCallFor records the operations that a replica executes.
func (s *Sim) upCallFor(id ReplicaNumber) UpCall {
	return func(op Payload) Payload {
		s.executed[id] = append(s.executed[id], op)
		return op
	}
}

func (s *Sim) WithFaults(faults Faults) *Sim {
	s.faults = faults
	return s
}

func (s *Sim) getActivePeers() []*Replica {
	peers := []*Replica{}
	for _, peer := range s.replicas {
		if s.isDown(peer.state.ReplicaNumber) {
			continue
		}

		peers = append(peers, peer)
	}

	return peers
}

func (s *Sim) getPeer(id ReplicaNumber) *Replica {
	for _, peer := range s.replicas {
		if peer.state.ReplicaNumber == id {
			return peer
		}
	}

	return nil
}

func (s *Sim) getPrimary() *Replica {
	for _, peer := range s.getActivePeers() {
		if peer.state.Role == RolePrimary {
			return peer
		}
	}

	return nil
}

func (s *Sim) pause(id ReplicaNumber) {
	s.down[id] = true
}

func (s *Sim) resume(id ReplicaNumber) {
	delete(s.down, id)
}

// crash stops a replica. Unlike pause, its state is lost: the only way back
// is restart, which starts it with an empty state.
func (s *Sim) crash(id ReplicaNumber) {
	s.pause(id)
}

// restart replaces a crashed replica with a new one that has the same
// number, an empty state and a new seed, and that starts in recovery.
func (s *Sim) restart(id ReplicaNumber) {
	s.t.Helper()
	if !s.isDown(id) {
		s.t.Fatalf("restart of replica %d, which did not crash", id)
	}

	// the new replica executes the ops again from the start
	s.executed[id] = nil
	s.checkedExec[id] = 0
	s.seenOps[id] = nil
	s.lastCommit[id] = 0

	s.replicas[id] = NewReplica(id, s.members, s.upCallFor(id), s.rand.Uint64(), true)
	s.resume(id)
}

func (s *Sim) isDown(id ReplicaNumber) bool {
	return s.down[id]
}

type Condition func() bool

func (s *Sim) sendNextTick(outgoing []Outgoing) {
	for _, msg := range outgoing {
		s.network = append(s.network, inFlight{
			deliverAt: s.now + 1 + s.faults.delay(s.rand),
			out:       msg,
		})
	}
}

// Send puts a message from outside the cluster, for example from a client,
// into the network.
func (s *Sim) Send(to ReplicaNumber, msg Message, deliveryAt int) {
	s.network = append(s.network, inFlight{
		deliverAt: deliveryAt + s.faults.delay(s.rand),
		out: Outgoing{
			To:  PeerCorrespondent(to),
			Msg: msg,
		},
	})
}

func (s *Sim) deliver() {
	s.t.Helper()

	newMsgs := []inFlight{}
	slices.SortStableFunc(s.network, func(a, b inFlight) int {
		return cmp.Compare(a.deliverAt, b.deliverAt)
	})

	i := 0
	for i = 0; i < len(s.network); i++ {
		msg := s.network[i]
		if msg.deliverAt > s.now {
			break
		}

		if msg.out.To.Type == CorrespondentPeer {
			peer := s.getPeer(msg.out.To.Peer)
			if s.isDown(peer.state.ReplicaNumber) {
				continue
			}
			if s.faults.shouldDrop(s.rand) {
				continue
			}

			msgs := peer.Step(msg.out.Msg)

			newInflights := make([]inFlight, len(msgs))
			for j, outGoingMsg := range msgs {
				newInflights[j] = inFlight{
					deliverAt: s.now + 1 + s.faults.delay(s.rand),
					out:       outGoingMsg,
				}
			}

			newMsgs = append(newMsgs, newInflights...)
			if s.faults.shouldDup(s.rand) {
				newMsgs = append(newMsgs, msg)
			}

		} else {
			s.replies = append(s.replies, msg.out.Msg.(*ClientReply))
		}
	}

	s.network = s.network[i:]
	s.network = append(s.network, newMsgs...)
}

func (s *Sim) tick() {
	s.t.Helper()

	s.now++

	s.deliver()
	for _, peer := range s.replicas {
		if s.isDown(peer.state.ReplicaNumber) {
			continue
		}

		out := peer.Tick()
		s.sendNextTick(out)
	}

	s.checkInvariants()
}

// checkInvariants fails the test if a safety property does not hold.
func (s *Sim) checkInvariants() {
	s.t.Helper()

	for _, r := range s.replicas {
		id := r.state.ReplicaNumber

		// at most one primary in each view
		if r.state.Role == RolePrimary && r.state.Status == StatusNormal {
			if p, ok := s.primaryOfView[r.state.ViewNumber]; ok && p != id {
				s.t.Fatalf("view %d has two primaries: %d and %d", r.state.ViewNumber, p, id)
			}
			s.primaryOfView[r.state.ViewNumber] = id
		}

		// the commit-number never decreases
		if r.state.CommitNumber < s.lastCommit[id] {
			s.t.Fatalf("replica %d commit went from %d to %d",
				id, s.lastCommit[id], r.state.CommitNumber)
		}
		s.lastCommit[id] = r.state.CommitNumber

		// all replicas execute the same sequence, and each op at most once
		if s.seenOps[id] == nil {
			s.seenOps[id] = map[string]bool{}
		}
		executed := s.executed[id]
		for i := s.checkedExec[id]; i < len(executed); i++ {
			op := executed[i]
			if i < len(s.committed) {
				if !bytes.Equal(op, s.committed[i]) {
					s.t.Fatalf("replica %d executed %q as op %d, another replica executed %q",
						id, op, i+1, s.committed[i])
				}
			} else {
				s.committed = append(s.committed, op)
			}
			if s.seenOps[id][string(op)] {
				s.t.Fatalf("replica %d executed %q twice", id, op)
			}
			s.seenOps[id][string(op)] = true
		}
		s.checkedExec[id] = len(executed)
	}
}

func (s *Sim) RunUntil(condition Condition, maxTicks int) {
	s.t.Helper()

	stopTicks := s.now + maxTicks
	for s.now < stopTicks {
		if condition() {
			break
		}
		s.tick()
	}

	if !condition() {
		s.t.Fatalf("condition not met after %d ticks", maxTicks)
	}
}

func (s *Sim) HasPrimary() bool {
	for _, peer := range s.getActivePeers() {
		if peer.state.Role == RolePrimary {
			return true
		}
	}

	return false
}

func (s *Sim) IsOperational() bool {
	primaries := 0
	backups := 0
	highestView := ViewNumber(0)

	for _, peer := range s.getActivePeers() {
		if peer.state.Role == RolePrimary {
			primaries++
		} else {
			backups++
		}
		if peer.state.ViewNumber > highestView {
			highestView = peer.state.ViewNumber
		}
	}

	if primaries != 1 {
		return false
	}

	for _, peer := range s.getActivePeers() {
		if peer.state.ViewNumber != highestView {
			return false
		}

		if peer.state.Status != StatusNormal {
			return false
		}
	}

	return true
}

func (s *Sim) AllNodesOperational() bool {
	primaries := 0
	backups := 0
	highestView := ViewNumber(0)
	for _, peer := range s.replicas {
		if peer.state.Role == RolePrimary {
			primaries++
		} else {
			backups++
		}
		if peer.state.ViewNumber > highestView {
			highestView = peer.state.ViewNumber
		}
	}

	if primaries != 1 {
		return false
	}

	for _, peer := range s.replicas {
		if peer.state.ViewNumber != highestView {
			return false
		}

		if peer.state.Status != StatusNormal {
			return false
		}
	}

	return true
}

func (s *Sim) IsCommitted(clientId ClientId, requestNumber RequestNumber) bool {
	primary := s.getPrimary()
	if primary == nil {
		return false
	}
	for logIndex, logEntry := range primary.state.Log {
		if logEntry.From == clientId && logEntry.RequestNumber == requestNumber {
			opNumber := logIndex + 1
			if primary.state.CommitNumber >= CommitNumber(opNumber) {
				return true
			}

			return false
		}
	}
	return false
}

func (s *Sim) IsResponded(requestNumber RequestNumber) bool {
	for _, reply := range s.replies {
		if reply.RequestNumber == requestNumber {
			return true
		}
	}

	return false
}

func (s *Sim) RequestUntilCommitted(client ClientId, req RequestNumber, op Payload, maxTicks int) {
	s.t.Helper()
	s.RunUntil(func() bool {
		primary := s.getPrimary()
		if primary == nil {
			return false
		}
		if s.IsCommitted(client, req) {
			return true
		}
		if s.now%5 == 0 {
			s.Send(primary.state.ReplicaNumber,
				&ClientRequest{From: client, RequestNumber: req, Operation: op},
				s.now+1)
		}
		return false
	}, maxTicks)
}

// AllCaughtUp is true when all replicas are normal in the same view, there is
// one primary, and each replica has the full log of that view.
func (s *Sim) AllCaughtUp() bool {
	if !s.AllNodesOperational() {
		return false
	}
	primary := s.getPrimary()
	for _, r := range s.replicas {
		if r.lastNormalView != r.state.ViewNumber ||
			r.state.CommitNumber != primary.state.CommitNumber {
			return false
		}
	}
	return true
}

var defaultFaults = Faults{
	MaxDelay: 2,
	DropRate: 0.05,
}

const fuzzCorpusSeeds = 20

// fuzzSim runs a scenario with faults. A normal `go test` runs the corpus
// seeds; `go test -fuzz` searches for new seeds.
func fuzzSim(f *testing.F, members int, faults Faults, scenario func(t *testing.T, s *Sim)) {
	for seed := range uint64(fuzzCorpusSeeds) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed uint64) {
		scenario(t, NewSim(t, members, seed).WithFaults(faults))
	})
}

// Initial view change

func TestInitialViewChange(t *testing.T) {
	s := NewSim(t, 3, 42)
	s.RunUntil(s.IsOperational, 100)
}

func FuzzInitialViewChange(f *testing.F) {
	fuzzSim(f, 3, defaultFaults, func(t *testing.T, s *Sim) {
		s.RunUntil(s.IsOperational, 200)
	})
}

// View change with one replica down

func viewChangeScenario(t *testing.T, s *Sim, strict bool) {
	s.RunUntil(s.IsOperational, 200)
	oldPrimary := s.getPrimary()

	s.pause(oldPrimary.state.ReplicaNumber)
	s.RunUntil(func() bool {
		p := s.getPrimary()
		return s.IsOperational() && p != nil && p != oldPrimary
	}, 200)
	newPrimary := s.getPrimary()

	s.resume(oldPrimary.state.ReplicaNumber)
	s.RunUntil(s.AllCaughtUp, 200)

	if strict && s.getPrimary() != newPrimary {
		t.Fatal("old primary coming back caused a view change")
	}
}

func TestViewChangeWithOneDown(t *testing.T) {
	viewChangeScenario(t, NewSim(t, 3, 42), true)
}

func FuzzViewChangeWithOneDown(f *testing.F) {
	fuzzSim(f, 3, defaultFaults, func(t *testing.T, s *Sim) {
		viewChangeScenario(t, s, false)
	})
}

// Processing a request

func processScenario(t *testing.T, s *Sim, strict bool) {
	s.RunUntil(s.IsOperational, 200)

	clientId := ClientId(555)
	reqId := RequestNumber(123)
	operation := []byte("lol")

	if strict {
		// without faults, one request is enough
		primary := s.getPrimary()
		s.Send(primary.state.ReplicaNumber,
			&ClientRequest{RequestNumber: reqId, From: clientId, Operation: operation},
			s.now+1)
		s.RunUntil(func() bool { return s.IsCommitted(clientId, reqId) }, 100)
	} else {
		s.RequestUntilCommitted(clientId, reqId, operation, 300)
	}

	primary := s.getPrimary()
	executed := s.executed[primary.state.ReplicaNumber]
	if len(executed) != 1 || !bytes.Equal(executed[0], operation) {
		t.Errorf("primary executed %q, want [%q]", executed, operation)
	}

	s.RunUntil(func() bool { return s.IsResponded(reqId) }, 100)
	if strict && len(s.replies) != 1 {
		t.Errorf("client got %d replies, want 1", len(s.replies))
	}
}

func TestProcess(t *testing.T) {
	processScenario(t, NewSim(t, 3, 42), true)
}

func FuzzProcess(f *testing.F) {
	fuzzSim(f, 3, defaultFaults, func(t *testing.T, s *Sim) {
		processScenario(t, s, false)
	})
}

// Random workload: requests, pauses and resumes in a random order

// randomWorkload runs requests, pauses and resumes in a random order, and
// returns the last request number that it used.
func randomWorkload(s *Sim, clientId ClientId, ticks int) RequestNumber {
	req := RequestNumber(0)
	maxPaused := len(s.replicas) / 2 // f

	for range ticks {
		switch n := s.rand.IntN(100); {
		case n < 10:
			if p := s.getPrimary(); p != nil {
				req++
				s.Send(p.state.ReplicaNumber,
					&ClientRequest{From: clientId, RequestNumber: req,
						Operation: fmt.Append(nil, req)},
					s.now+1)
			}
		case n < 12:
			if len(s.down) < maxPaused {
				s.pause(ReplicaNumber(s.rand.IntN(len(s.replicas))))
			}
		case n < 14:
			// sorted, so that the seed alone selects the replica
			if paused := slices.Sorted(maps.Keys(s.down)); len(paused) > 0 {
				s.resume(paused[0])
			}
		}
		s.tick()
	}

	return req
}

// randomScenario runs a random workload, then heals the cluster. The cluster
// must recover and accept a new request.
func randomScenario(t *testing.T, s *Sim) {
	clientId := ClientId(1)
	req := randomWorkload(s, clientId, 1000)

	for _, r := range s.replicas {
		s.resume(r.state.ReplicaNumber)
	}
	s.RunUntil(s.AllCaughtUp, 500)
	req++
	s.RequestUntilCommitted(clientId, req, fmt.Append(nil, req), 500)
}

func FuzzRandom(f *testing.F) {
	fuzzSim(f, 3, defaultFaults, randomScenario)
}

// Duplicated messages arrive late and out of order, so they test the view
// checks and the handling of old messages.
func FuzzRandomWithDuplicates(f *testing.F) {
	fuzzSim(f, 3, Faults{MaxDelay: 2, DropRate: 0.05, DupRate: 0.05}, randomScenario)
}

// simTrace sends groups of requests, so that several ops are in flight at
// the same time, and records the state of each replica after each tick.
func simTrace(t *testing.T, seed uint64) []string {
	s := NewSim(t, 3, seed).WithFaults(defaultFaults)
	s.RunUntil(s.IsOperational, 200)

	req := RequestNumber(0)
	trace := []string{}
	for tick := range 300 {
		if p := s.getPrimary(); p != nil && tick%10 == 0 {
			for range 5 {
				req++
				s.Send(p.state.ReplicaNumber,
					&ClientRequest{From: 1, RequestNumber: req,
						Operation: fmt.Append(nil, req)},
					s.now+1)
			}
		}
		s.tick()

		line := ""
		for _, r := range s.replicas {
			line += fmt.Sprintf("r%d v%d s%d op%d c%d; ", r.state.ReplicaNumber,
				r.state.ViewNumber, r.state.Status, r.state.OpNumber, r.state.CommitNumber)
		}
		trace = append(trace, line+fmt.Sprint(len(s.network)))
	}
	return trace
}

// A seed must repeat the same run. Otherwise a seed that the fuzzer saves
// does not reproduce its failure. A common cause is a range over a map that
// sends messages: Go chooses a random order for each such loop.
func TestSimIsDeterministic(t *testing.T) {
	for seed := range uint64(50) {
		first, second := simTrace(t, seed), simTrace(t, seed)
		for tick := range first {
			if first[tick] != second[tick] {
				t.Fatalf("seed %d differs at tick %d:\n  run 1: %s\n  run 2: %s",
					seed, tick+1, first[tick], second[tick])
			}
		}
	}
}

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.DiscardHandler))
	os.Exit(m.Run())
}
