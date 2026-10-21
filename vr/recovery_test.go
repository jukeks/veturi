package vr

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"testing"
)

// A backup crashes after 5 committed requests and loses its state. After a
// restart it must recover all 5 operations, in the same order, without a
// view change.
func recoveryScenario(t *testing.T, s *Sim, strict bool) {
	s.RunUntil(s.IsOperational, 200)
	view := s.getPrimary().state.ViewNumber

	clientId := ClientId(555)
	want := []Payload{}
	for req := RequestNumber(1); req <= 5; req++ {
		op := fmt.Appendf(nil, "op %d", req)
		want = append(want, op)
		s.RequestUntilCommitted(clientId, req, op, 300)
	}

	var crashed *Replica
	for _, r := range s.replicas {
		if r.state.Role == RoleBackup {
			crashed = r
			break
		}
	}
	id := crashed.state.ReplicaNumber

	s.crash(id)
	for range 5 {
		s.tick() // the cluster continues without the crashed replica
	}
	s.restart(id)
	restarted := s.replicas[id]
	if restarted.state.Status != StatusRecovering {
		t.Fatalf("restarted replica status %d, want recovering", restarted.state.Status)
	}

	s.RunUntil(s.AllCaughtUp, 300)

	got := s.executed[id]
	if len(got) != len(want) {
		t.Fatalf("restarted replica executed %d ops, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Errorf("op %d: restarted replica executed %q, want %q", i+1, got[i], want[i])
		}
	}
	if restarted.state.Role != RoleBackup {
		t.Errorf("restarted replica role %d, want backup", restarted.state.Role)
	}
	if strict && restarted.state.ViewNumber != view {
		t.Errorf("view %d after recovery, want %d: recovery caused a view change",
			restarted.state.ViewNumber, view)
	}
}

func TestRecovery(t *testing.T) {
	recoveryScenario(t, NewSim(t, 3, 42), true)
}

func FuzzRecovery(f *testing.F) {
	fuzzSim(f, 3, defaultFaults, func(t *testing.T, s *Sim) {
		recoveryScenario(t, s, false)
	})
}

// The primary crashes after 5 committed requests. The others elect a new
// primary and commit one more request. The old primary restarts and must
// recover all 6 operations as a backup.
func crashPrimaryScenario(t *testing.T, s *Sim) {
	s.RunUntil(s.IsOperational, 200)

	clientId := ClientId(555)
	for req := RequestNumber(1); req <= 5; req++ {
		s.RequestUntilCommitted(clientId, req, fmt.Appendf(nil, "op %d", req), 300)
	}

	oldPrimary := s.getPrimary().state.ReplicaNumber
	s.crash(oldPrimary)
	s.RunUntil(func() bool {
		p := s.getPrimary()
		return s.IsOperational() && p != nil && p.state.ReplicaNumber != oldPrimary
	}, 300)
	s.RequestUntilCommitted(clientId, 6, fmt.Appendf(nil, "op %d", 6), 300)

	s.restart(oldPrimary)
	s.RunUntil(s.AllCaughtUp, 300)

	if n := len(s.executed[oldPrimary]); n != 6 {
		t.Fatalf("restarted old primary executed %d ops, want 6", n)
	}
	if role := s.replicas[oldPrimary].state.Role; role != RoleBackup {
		t.Errorf("restarted old primary role %d, want backup", role)
	}
}

func TestRecoveryOfPrimary(t *testing.T) {
	crashPrimaryScenario(t, NewSim(t, 3, 42))
}

func FuzzRecoveryOfPrimary(f *testing.F) {
	fuzzSim(f, 3, defaultFaults, crashPrimaryScenario)
}

// failedCount is the number of replicas that cannot vote: crashed or paused
// replicas, and replicas that are still recovering.
func (s *Sim) failedCount() int {
	failed := len(s.down)
	for _, r := range s.getActivePeers() {
		if r.state.Status == StatusRecovering {
			failed++
		}
	}
	return failed
}

// randomCrashWorkload runs requests, crashes and restarts in a random order.
// It crashes a replica only while fewer than f replicas have failed: a
// replica that is still recovering counts as failed, because it cannot vote.
// With more failures VR blocks, and that is the correct behavior.
func randomCrashWorkload(s *Sim, clientId ClientId, ticks int) RequestNumber {
	req := RequestNumber(0)
	f := len(s.replicas) / 2

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
			if s.failedCount() < f {
				s.crash(ReplicaNumber(s.rand.IntN(len(s.replicas))))
			}
		case n < 14:
			// sorted, so that the seed alone selects the replica
			if crashed := slices.Sorted(maps.Keys(s.down)); len(crashed) > 0 {
				s.restart(crashed[0])
			}
		}
		s.tick()
	}

	return req
}

func FuzzRandomCrash(f *testing.F) {
	fuzzSim(f, 3, defaultFaults, func(t *testing.T, s *Sim) {
		clientId := ClientId(1)
		req := randomCrashWorkload(s, clientId, 1000)

		for _, id := range slices.Sorted(maps.Keys(s.down)) {
			s.restart(id)
		}
		s.RunUntil(s.AllCaughtUp, 1000)
		req++
		s.RequestUntilCommitted(clientId, req, fmt.Append(nil, req), 500)
	})
}
