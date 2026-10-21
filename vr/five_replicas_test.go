package vr

import (
	"fmt"
	"testing"
)

// Clusters of 5 replicas have f = 2. A quorum is 3 replicas, so the cluster
// must continue with 2 failed replicas, and it must block with 3.

const fiveReplicas = 5

// backups returns the IDs of the active backups, in replica order.
func (s *Sim) backups() []ReplicaNumber {
	ids := []ReplicaNumber{}
	for _, r := range s.getActivePeers() {
		if r.state.Role == RoleBackup {
			ids = append(ids, r.state.ReplicaNumber)
		}
	}
	return ids
}

// The scenarios for 3 replicas also work for 5 replicas.

func TestInitialViewChangeFive(t *testing.T) {
	s := NewSim(t, fiveReplicas, 42)
	s.RunUntil(s.IsOperational, 100)
}

func FuzzInitialViewChangeFive(f *testing.F) {
	fuzzSim(f, fiveReplicas, defaultFaults, func(t *testing.T, s *Sim) {
		s.RunUntil(s.IsOperational, 200)
	})
}

func TestProcessFive(t *testing.T) {
	processScenario(t, NewSim(t, fiveReplicas, 42), true)
}

func FuzzProcessFive(f *testing.F) {
	fuzzSim(f, fiveReplicas, defaultFaults, func(t *testing.T, s *Sim) {
		processScenario(t, s, false)
	})
}

func TestViewChangeWithOneDownFive(t *testing.T) {
	viewChangeScenario(t, NewSim(t, fiveReplicas, 42), true)
}

func TestStateTransferFive(t *testing.T) {
	stateTransferScenario(t, NewSim(t, fiveReplicas, 42), true)
}

func TestRecoveryFive(t *testing.T) {
	recoveryScenario(t, NewSim(t, fiveReplicas, 42), true)
}

func TestRecoveryOfPrimaryFive(t *testing.T) {
	crashPrimaryScenario(t, NewSim(t, fiveReplicas, 42))
}

func FuzzRandomFive(f *testing.F) {
	fuzzSim(f, fiveReplicas, defaultFaults, randomScenario)
}

func FuzzRandomWithDuplicatesFive(f *testing.F) {
	fuzzSim(f, fiveReplicas, Faults{MaxDelay: 2, DropRate: 0.05, DupRate: 0.05}, randomScenario)
}

func FuzzRandomCrashFive(f *testing.F) {
	fuzzSim(f, fiveReplicas, defaultFaults, randomCrashScenario)
}

// Two primaries fail, one after the other. The 3 replicas that are left must
// elect a third primary and commit a request. Then the two old primaries
// come back and must catch up.
func twoPrimariesDownScenario(t *testing.T, s *Sim) {
	s.RunUntil(s.IsOperational, 200)
	clientId := ClientId(555)
	s.RequestUntilCommitted(clientId, 1, []byte("op 1"), 300)

	paused := []ReplicaNumber{}
	for req := RequestNumber(2); req <= 3; req++ {
		old := s.getPrimary().state.ReplicaNumber
		s.pause(old)
		paused = append(paused, old)
		s.RunUntil(func() bool {
			p := s.getPrimary()
			return s.IsOperational() && p != nil && p.state.ReplicaNumber != old
		}, 300)
		s.RequestUntilCommitted(clientId, req, fmt.Appendf(nil, "op %d", req), 300)
	}

	for _, id := range paused {
		s.resume(id)
	}
	s.RunUntil(s.AllCaughtUp, 300)

	for _, id := range paused {
		if n := len(s.executed[id]); n != 3 {
			t.Errorf("old primary %d executed %d ops, want 3", id, n)
		}
	}
}

func TestTwoPrimariesDownFive(t *testing.T) {
	twoPrimariesDownScenario(t, NewSim(t, fiveReplicas, 42))
}

func FuzzTwoPrimariesDownFive(f *testing.F) {
	fuzzSim(f, fiveReplicas, defaultFaults, twoPrimariesDownScenario)
}

// With 3 of 5 replicas down, the primary has no quorum, so it must not
// commit. When one replica comes back, the request must commit.
func noQuorumScenario(t *testing.T, s *Sim) {
	s.RunUntil(s.IsOperational, 200)
	clientId := ClientId(555)
	s.RequestUntilCommitted(clientId, 1, []byte("op 1"), 300)

	backups := s.backups()
	for _, id := range backups[:3] {
		s.pause(id)
	}

	primary := s.getPrimary()
	for range 100 {
		if s.now%5 == 0 {
			s.Send(primary.state.ReplicaNumber,
				&ClientRequest{From: clientId, RequestNumber: 2, Operation: []byte("op 2")},
				s.now+1)
		}
		s.tick()
	}
	if s.IsCommitted(clientId, 2) {
		t.Fatal("request committed with 2 of 5 replicas")
	}
	for _, r := range s.getActivePeers() {
		if r.state.CommitNumber > 1 {
			t.Fatalf("replica %d commit %d with 2 of 5 replicas, want 1",
				r.state.ReplicaNumber, r.state.CommitNumber)
		}
	}

	s.resume(backups[0])
	s.RequestUntilCommitted(clientId, 2, []byte("op 2"), 300)

	for _, id := range backups[1:3] {
		s.resume(id)
	}
	s.RunUntil(s.AllCaughtUp, 300)
}

func TestNoQuorumFive(t *testing.T) {
	noQuorumScenario(t, NewSim(t, fiveReplicas, 42))
}

func FuzzNoQuorumFive(f *testing.F) {
	fuzzSim(f, fiveReplicas, defaultFaults, noQuorumScenario)
}

// Two backups crash at the same time and recover in parallel. Each one needs
// 3 responses, and the 3 replicas that did not crash can give them.
func twoRecoveriesScenario(t *testing.T, s *Sim) {
	s.RunUntil(s.IsOperational, 200)
	clientId := ClientId(555)
	for req := RequestNumber(1); req <= 5; req++ {
		s.RequestUntilCommitted(clientId, req, fmt.Appendf(nil, "op %d", req), 300)
	}

	crashed := s.backups()[:2]
	for _, id := range crashed {
		s.crash(id)
	}
	s.RequestUntilCommitted(clientId, 6, []byte("op 6"), 300)

	for _, id := range crashed {
		s.restart(id)
	}
	s.RunUntil(s.AllCaughtUp, 300)

	for _, id := range crashed {
		if n := len(s.executed[id]); n != 6 {
			t.Errorf("recovered replica %d executed %d ops, want 6", id, n)
		}
	}
}

func TestTwoRecoveriesFive(t *testing.T) {
	twoRecoveriesScenario(t, NewSim(t, fiveReplicas, 42))
}

func FuzzTwoRecoveriesFive(f *testing.F) {
	fuzzSim(f, fiveReplicas, defaultFaults, twoRecoveriesScenario)
}
